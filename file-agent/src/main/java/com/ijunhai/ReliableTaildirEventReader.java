package com.ijunhai;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import com.ijunhai.emitter.ComposeEmitter;
import com.ijunhai.emitter.Emitter;
import com.ijunhai.metric.CollectMetrics;
import com.ijunhai.metric.MetricType;
import com.ijunhai.monitor.CollectMonitor;
import com.ijunhai.monitor.MonitorScheduler;
import com.ijunhai.util.PropertiesUtils;
import com.ijunhai.util.WinFileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.sql.SQLException;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.ijunhai.TaildirConstants.*;
import static com.ijunhai.metric.CollectEvent.HOST;
import static com.ijunhai.serde.MessageSerde.MAX_DATA_SYSTEM_SIZE;

public class ReliableTaildirEventReader {
    private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);
    private static final String OS_NAME = System.getProperty("os.name").toLowerCase();

    private static final int PACKAGE_BUFFER_SIZE = 1024*1024;

    private Map<Long, File> inodePath = Maps.newConcurrentMap();
    private Map<Long, CollectMetrics> inodeCollectMonitor = Maps.newConcurrentMap();
    private Map<String, CopyOnWriteArrayList<Long>> topicInodes = Maps.newConcurrentMap();

    private final Map<String, String> topicDataSystemMap;
    private final int batchSize;
    private final Map<String, List<String>> topicDirMap;
    private final List<String> topicGroup;
    private final Map<String, Pattern> topicIncludePatternMap;
    private final Map<String, Pattern> topicExcludePatternMap;
    private final Integer maxTailSeconds;
    private final RocketMQProducer producer;
    private final MonitorScheduler monitorScheduler;
    private final RateLimiter limiter;
    private final long fileLastModifyMillis;
    private final int fileRefreshPeroidSeconds;
    private final String host = PropertiesUtils.get(HOST);
    // Three thread:
    // tailFilesThread loop reading file
    // refreshFilesThread find new file
    // clearFilesThread remove expired file
    private final ExecutorService tailFilesThread;
    private final ScheduledExecutorService refreshFilesThread;
    private final ScheduledExecutorService clearFilesThread;

    private Cache<Long, TailFile> tailFileCache;

    private volatile boolean reading = false;

    private PositionStorage positionStorage;

    public ReliableTaildirEventReader(final File positionFile, final long fileLastModifyMillis) throws MQClientException, IOException, SQLException, ClassNotFoundException {
        Preconditions.checkNotNull(positionFile, "positionFile cannot be null.");
        Preconditions.checkState(positionFile.getParentFile().exists(), "positionFile's parent is not exists");
        String includeFileNamePattern = PropertiesUtils.get(INCLUDE_FILENAME_PATTERN);
        Preconditions.checkNotNull(includeFileNamePattern, "includeFileNamePattern cannot be null.");
        String excludeFileNamePattern = PropertiesUtils.get(EXCLUDE_FILENAME_PATTERN);
        this.fileLastModifyMillis = fileLastModifyMillis;
        this.fileRefreshPeroidSeconds = PropertiesUtils.getInt(FILE_REFRESH_PEROID_SECONDS, 20);

        String nameServer = PropertiesUtils.get(ROCKETMQ_NAMESERVER);
        String producerGroup = PropertiesUtils.get(ROCKETMQ_PRODUCERGROUP);
        Preconditions.checkNotNull(nameServer, "rocketmq.nameServer cannot be null.");
        Preconditions.checkNotNull(producerGroup, "rocketmq.producerGroup cannot be null.");

        this.maxTailSeconds = PropertiesUtils.getInt(PROCESS_SECONDS, 10);
        this.batchSize = PropertiesUtils.getInt(BATCH_SIZE_BYTES, PACKAGE_BUFFER_SIZE);
        this.positionStorage = new PositionStorage(positionFile);

        int retryTimes = PropertiesUtils.getInt(ROCKETMQ_SEND_RETRYTIMES, 1);

        String dirGroup = PropertiesUtils.get(TAIL_DIRS);
        Preconditions.checkNotNull(dirGroup, "tail.dirs cannot be null.");
        List<String> dirList = JSON.parseArray(dirGroup, String.class);

        String topicGroup = PropertiesUtils.get(TOPICS);
        Preconditions.checkNotNull(dirGroup, "topics cannot be null.");
        this.topicGroup = JSON.parseArray(topicGroup, String.class);

        String systems = PropertiesUtils.get(BUSINESS_SYSTEM);
        Preconditions.checkNotNull(dirGroup, "business.systems cannot be null.");
        List<String> systemList = JSON.parseArray(systems, String.class);

        Preconditions.checkState(dirList.size() == this.topicGroup.size(), "dirGroup size is not equal topicGroup size");
        Preconditions.checkState(systemList.size() == this.topicGroup.size(), "business systems size is not equal topicGroup size");
        topicDirMap = new HashMap<>();
        topicDataSystemMap = new HashMap<>();
        for (int i = 0; i < this.topicGroup.size(); i++) {
            final int index = i;
            String topic = this.topicGroup.get(i);
            List<String> paths = Lists.newArrayList(dirList.get(index).split(","));
            topicDirMap.put(topic, paths);
            topicDataSystemMap.put(topic, systemList.get(i));
        }

        this.topicIncludePatternMap = getTopicPatternMap(this.topicGroup, JSON.parseArray(includeFileNamePattern, String.class));

        if (StringUtils.isBlank(excludeFileNamePattern)) {
            this.topicExcludePatternMap = null;
        } else {
            this.topicExcludePatternMap = getTopicPatternMap(this.topicGroup, JSON.parseArray(excludeFileNamePattern, String.class));
        }

        this.producer = new RocketMQProducer(nameServer, producerGroup, retryTimes, positionStorage);

        this.limiter = RateLimiter.create(PropertiesUtils.getLong(FILE_READ_RATELIMITER, (long)PACKAGE_BUFFER_SIZE));
        final Emitter emitter = new ComposeEmitter(PropertiesUtils.get("emitter"));
        monitorScheduler = new MonitorScheduler(Executors.newSingleThreadScheduledExecutor(), emitter);

        this.tailFilesThread = Executors.newCachedThreadPool();
        this.refreshFilesThread = Executors.newScheduledThreadPool(1);
        this.clearFilesThread = Executors.newScheduledThreadPool(1);

    }


    public List<Future> start() throws IOException, SQLException {
        final List<Future> futures = Lists.newArrayList();

        refreshFiles();

        positionStorage.start();

        tailFileCache = CacheBuilder.newBuilder()
                .expireAfterAccess(1, TimeUnit.DAYS)
                .maximumSize(1000)
                .removalListener(new RemovalListener<Long, TailFile>() {
                    @Override
                    public void onRemoval(RemovalNotification<Long, TailFile> notification) {
                        notification.getValue().close();
                    }
                })
                .build();

        clearFilesThread.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    clearFiles();
                } catch (Exception e) {
                }
            }
        }, 3600, 3600, TimeUnit.SECONDS);

        refreshFilesThread.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    refreshFiles();
                } catch (Exception ignored) {
                }
            }
        }, fileRefreshPeroidSeconds, fileRefreshPeroidSeconds, TimeUnit.SECONDS);

        reading = true;

        topicGroup.forEach(topic -> {
            ByteBuffer readedBuffer = ByteBuffer.allocateDirect(batchSize + 16 + MAX_DATA_SYSTEM_SIZE);
            logger.info("allocated direct byte buffer {}", readedBuffer.capacity());
            Future future = tailFilesThread.submit(new Runnable() {
                @Override
                public void run() {
                    processTail(topic, readedBuffer);
                }
            });
            futures.add(future);
        });
        monitorScheduler.start();
        return futures;
    }

    public void stop() {
        reading = false;
        try {
            Thread.sleep(2000);
        } catch (Exception ex) {
            logger.warn("sleep Exception", ex);
        }
        try {
            refreshFilesThread.shutdown();
        } catch (Exception ignored) {}
        try {
            clearFilesThread.shutdown();
        } catch (Exception ignored) {}
        try {
            tailFilesThread.shutdown();
        } catch (Exception ignored) {}

        monitorScheduler.stop();
        producer.close();
        try {
            Thread.sleep(2000);
        } catch (Exception ex) {
            logger.warn("sleep Exception", ex);
        }
    }


    public void processTail(final String topic, ByteBuffer readedBuffer) {
        while (reading) {
            List<Long> inodes = topicInodes.get(topic);
            List<Long> readList = inodes.subList(0, inodes.size());

            // group by date and sort by date
            Map<String, List<Long>> dateInodes = new TreeMap<>(readList.stream().collect(Collectors.groupingBy(new Function<Long, String>() {
                @Override
                public String apply(Long inode) {
                    long millseconds = inodePath.get(inode).lastModified();
                    String date = Instant.ofEpochMilli(millseconds).atZone(ZoneId.systemDefault()).toLocalDate().toString();
                    return date;
                }
            })));
            // read files by date
            for (Map.Entry<String, List<Long>> entry : dateInodes.entrySet()) {
                List<Long> toReadList = entry.getValue();
                while (!isComplete(topic, toReadList)) {
                    for (Long inode : toReadList) {
                        if (!reading) {
                            logger.warn("Stopped reading...");
                            return;
                        }
                        final File file = inodePath.get(inode);
                        if (file == null) {
                            logger.warn("the file [inode: {}] is null", inode);
                            continue;
                        }
                        try {
                            TailFile tailFile = getTailFile(topic, inode);
                            if (tailFile == null || !tailFile.needRead()) {
                                continue;
                            }

                            if (readedBuffer.position() == 0) {
                                tailFile.tail(readedBuffer);
                                inodeCollectMonitor.get(inode).incrementLogReaded(tailFile.getCurrentLineCount());
                            }

                            while (readedBuffer.position() > 0) {
                                // limit send speed
                                limiter.acquire(readedBuffer.position());
                                if (producer.sendMesage(topic, inode, tailFile.getFileInfo(), readedBuffer,
                                        tailFile.getCurrentLineCount(), topicDataSystemMap.get(topic))) {
                                    inodeCollectMonitor.get(inode).incrementSended(tailFile.getCurrentLineCountAndSet(0));
                                } else {
                                    logger.warn("send message fail, will try again");
                                    try {
                                        TimeUnit.MILLISECONDS.sleep(1000);
                                    } catch (Exception ex) {
                                        logger.warn("sleep Exception", ex);
                                    }
                                }
                            }
                        } catch (Exception ex) {}
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(500);
                    } catch (Exception ex) {
                        logger.warn("sleep Exception", ex);
                    }
                }

            }

            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (Exception ex) {
                logger.warn("sleep Exception", ex);
            }
        }
    }

    private TailFile getTailFile(final String topic, final Long inode) throws ExecutionException {
        return tailFileCache.get(inode, new Callable<TailFile>() {
            @Override
            public TailFile call() throws Exception {
                File file = inodePath.get(inode);
                if (file != null) {
                    FileInfo fileInfo = positionStorage.getFileInfo(inode);
                    if (fileInfo == null) {
                        fileInfo = new FileInfo(host, file.getAbsolutePath(), null, 0);
                    }
                    logger.info("Get file [{}], from position [{}]", file.getAbsolutePath(), fileInfo.getPosition());
                    CollectMonitor monitor = new CollectMonitor(host, topic, file.getAbsolutePath(), MetricType.LogFile, new CollectMetrics());
                    monitorScheduler.addMonitor(monitor);
                    inodeCollectMonitor.put(inode, monitor.getCollectMetrics());
                    return new TailFile(fileInfo, batchSize, maxTailSeconds);
                }
                logger.warn("the file [inode: {}] is null", inode);
                return null;
            }
        });
    }

    private boolean isComplete(String topic, List<Long> inodes) {
        for (Long inode : inodes) {
            try {
                TailFile tailFile = getTailFile(topic, inode);
                if (!tailFile.isComplete()) {
                    return false;
                }
            } catch (Exception e) {
            }
        }
        return true;
    }

    public void clearFiles() {
        Iterator<Map.Entry<Long, File>> it = inodePath.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, File> entry = it.next();
            if (!entry.getValue().exists()) {
                logger.info("remove deleted file: " + entry.getValue().getAbsolutePath());
                it.remove();
                try {
                    topicInodes.values().parallelStream().forEach(list -> {
                        if (list.contains(entry.getKey())) {
                            list.remove(entry.getKey());
                        }
                    });
                } catch (Exception ignore) {}
                try {
                    positionStorage.deleteFileInfo(entry.getKey());
                } catch (Exception ignored) {
                }
            }

        }

    }

    public void refreshFiles() throws IOException {
        topicDirMap.forEach((topic, paths) -> {

            CopyOnWriteArrayList<Long> inodes = topicInodes.getOrDefault(topic, new CopyOnWriteArrayList<>());
            paths.forEach(path -> {
                File dir = new File(path);
                if (dir.exists()) {
                    try {
                        Files.walk(dir.toPath(), 1).filter(p -> {
                            try {
                                TimeUnit.MILLISECONDS.sleep(10);
                            } catch (Exception ex) {
                                logger.warn("sleep Exception", ex);
                            }
                            if (!Files.isRegularFile(p)) {
                                return false;
                            }
                            File file = p.toFile();
                            long lastModified = file.lastModified();

                            Long inode = getInode(file);
                            if (inode == null || inodePath.containsKey(inode)) {
                                return false;
                            }

                            if (lastModified < fileLastModifyMillis) {
                                return false;
                            }

                            if (topicExcludePatternMap != null && topicExcludePatternMap.get(topic).matcher(file.getName()).matches()) {
                                return false;
                            }
                            Matcher matcher = topicIncludePatternMap.get(topic).matcher(file.getName());
                            if (matcher.matches()) {
                                logger.info("find new file: " + file.getAbsolutePath());
                                inodePath.put(inode, file);
                                inodes.add(inode);
                            }
                            return matcher.matches();
                        }).count();
                    } catch (IOException e) {
                    }
                } else {
                    logger.warn("Cannot find dir: [%s]", dir);
                }
            });
            topicInodes.put(topic, inodes);
        });
    }

    private Long getInode(File file) {
        try {
            if (OS_NAME.contains("windows")) {
                return Long.parseLong(WinFileUtils.getFileId(file.toPath().toString()));
            } else {
                return (Long) Files.getAttribute(file.toPath(), "unix:ino");
            }
        } catch (IOException ex) {
            logger.error("Cannot get inode for [%s].", file.getAbsolutePath());
            return null;
        }
    }

    private Map<String, Pattern> getTopicPatternMap(List<String> topics, List<String> patternStrings) {
        Preconditions.checkState(topics.size() == patternStrings.size(), "topics size is not equal patterns size");
        Map<String, Pattern> topicPatternMap = Maps.newHashMap();
        for (int i = 0; i < topics.size(); i++) {
            String patternString = patternStrings.get(i);
            Pattern pattern = Pattern.compile(patternString);
            topicPatternMap.put(topics.get(i), pattern);
        }
        return topicPatternMap;
    }
}
