package com.ijunhai.derby;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.skife.jdbi.v2.Batch;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.exceptions.DBIException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.concurrent.Callable;

public class DerbyHelper {

    private static final Logger logger = LoggerFactory.getLogger(DerbyHelper.class);

    private volatile boolean running = false;

    private final static int DEFAULT_MAX_TRIES = 3;

    private DBI dbi;

    private File derbyDir;

    private final Predicate<Throwable> shouldRetry;

    public DerbyHelper(File derbyDir) {
        this.derbyDir = derbyDir;
        this.shouldRetry = new Predicate<Throwable>()
        {
            @Override
            public boolean apply(Throwable e)
            {
                return isTransientException(e);
            }
        };
    }



    public void start() {
        logger.info("data initing ...");
        System.setProperty("derby.system.home", derbyDir.getAbsolutePath());
        System.setProperty("derby.stream.error.method",
                "com.ijunhai.derby.DerbyLogOutputStream.disableLog");
        System.setProperty("derby.storage.logArchiveMode", "true");
        if (!running) {
            EmbeddedDataSource newDataSource = new EmbeddedDataSource();
            newDataSource.setCreateDatabase("create");
            newDataSource.setDatabaseName(derbyDir.getAbsolutePath()+"/position");
            dbi = new DBI(newDataSource);
            running = true;
        }
    }

    public DBI getDBI() {
        return dbi;
    }


    public <T> T retryWithHandle(
            final HandleCallback<T> callback,
            final Predicate<Throwable> myShouldRetry
    )
    {
        final Callable<T> call = new Callable<T>()
        {
            @Override
            public T call() throws Exception
            {
                return getDBI().withHandle(callback);
            }
        };
        try {
            return RetryUtils.retry(call, myShouldRetry, DEFAULT_MAX_TRIES);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public <T> T retryWithHandle(final HandleCallback<T> callback)
    {
        return retryWithHandle(callback, shouldRetry);
    }

    public <T> T retryTransaction(final TransactionCallback<T> callback, final int quietTries, final int maxTries)
    {
        final Callable<T> call = new Callable<T>()
        {
            @Override
            public T call() throws Exception
            {
                return getDBI().inTransaction(callback);
            }
        };
        try {
            return RetryUtils.retry(call, shouldRetry, quietTries, maxTries);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public void createTable(final String tableName, final Iterable<String> sql)
    {
        try {
            retryWithHandle(
                    new HandleCallback<Void>()
                    {
                        @Override
                        public Void withHandle(Handle handle) throws Exception
                        {
                            if (!tableExists(handle, tableName)) {
                                logger.info("Creating table[{}]", tableName);
                                final Batch batch = handle.createBatch();
                                for (String s : sql) {
                                    batch.add(s);
                                }
                                batch.execute();
                            } else {
                                logger.info("Table[{}] already exists", tableName);
                            }
                            return null;
                        }
                    }
            );
        }
        catch (Exception e) {
            logger.warn("Exception creating table", e);
        }
    }

    public boolean tableExists(Handle handle, String tableName)
    {
        return !handle.createQuery("select * from SYS.SYSTABLES where tablename = :tableName")
                .bind("tableName", tableName.toUpperCase())
                .list()
                .isEmpty();
    }

    public final boolean isTransientException(Throwable e)
    {
        return e != null && (e instanceof SQLTransientException
                || e instanceof SQLRecoverableException
                || e instanceof UnableToObtainConnectionException
                || e instanceof UnableToExecuteStatementException
                || (e instanceof SQLException && isTransientException(e.getCause()))
                || (e instanceof DBIException && isTransientException(e.getCause())));
    }
}
