package com.howbuy.oracle2hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HTableWriter implements Closeable{

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	/*

	private static final Logger logger = LoggerFactory.getLogger(HTableWriter.class);

	private static final long DEFAULT_WRITE_BUFFER_SIZE = 3L * 1024 * 1024;

	private static final int DEFAULT_MAX_QUEUE_LENGTH = 10000;

	private static final String BATCHER_NAME_PREFIX = "Batch$";

	public HTableWriter(String tableName, int parallelNum) throws IOException {
		this(HBaseConfiguration.create(), tableName, parallelNum);
	}

	public HTableWriter(Properties config, String tableName, int parallelNum) throws IOException {
		this(Utils.createConfiguration(config), tableName, parallelNum);
	}

	public HTableWriter(Configuration configuration, String tableName, int parallelNum) throws IOException {
		if (parallelNum < 0) {
			throw new IllegalArgumentException("Invalid parameter 'parallelNum' for: " + parallelNum);
		}
		htables = new HTableInterface[parallelNum];
		for (int i = 0; i < parallelNum; i++) {
			htables[i] = createHTable(configuration, tableName);
			htables[i].setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
			htables[i].setAutoFlush(false);
		}
		this.workerExecutor = Executors.newFixedThreadPool(parallelNum);
	}

	private final HTableInterface[] htables;

	private final Executor workerExecutor;

	private final AtomicInteger lines = new AtomicInteger(0);

	private final AtomicInteger counter = new AtomicInteger(0);

	private final Queue<Put> queue = new ConcurrentLinkedQueue<Put>();

	private final ConcurrentMap<Integer, Batch> batchers = new ConcurrentHashMap<Integer, Batch>();

	private volatile int index = 1;

	private int batchSize = 100;

	private int pollSize = 1;

	private int maxQueueLength = DEFAULT_MAX_QUEUE_LENGTH;

	public long reset() {
		long l = lines.get();
		lines.set(0);
		return l;
	}

	public void setMaxQueueLength(int maxQueueLength) {
		if (maxQueueLength < DEFAULT_MAX_QUEUE_LENGTH) {
			this.maxQueueLength = maxQueueLength;
		}
	}

	public void setPollSize(int pollSize) {
		if (pollSize < batchSize / 2) {
			this.pollSize = pollSize;
		}
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	private class Batch implements Runnable {

		Batch(String name, HTableInterface ref) {
			this.name = name;
			this.ref = ref;
		}

		private String name;

		private final HTableInterface ref;

		private final List<Put> cache = new CopyOnWriteArrayList<Put>();

		public void run() {
			try {
				if (pollSize < 1) {
					for (int i = 0; i < RandomUtils.randomInt(1, batchSize / 2); i++) {
						Put put = queue.poll();
						if (put == null) {
							break;
						}
						cache.add(put);
					}
				} else {
					for (int i = 0; i < pollSize; i++) {
						Put put = queue.poll();
						if (put == null) {
							break;
						}
						cache.add(put);
					}
				}
				if (cache.size() >= batchSize) {
					doFlush();
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
			counter.decrementAndGet();
		}

		int size() {
			return cache.size();
		}

		boolean isEmpty() {
			return size() == 0;
		}

		void doFlush() throws IOException {
			synchronized (ref) {
				List<Put> list = new ArrayList<Put>(cache);
				ref.put(list);
				cache.removeAll(list);
				ref.flushCommits();
				lines.addAndGet(list.size());
				if (logger.isDebugEnabled()) {
					logger.debug(name + " flush to htable: " + cache.size() + "/" + queue.size() + "/" + lines.get());
				}
			}
		}

	}

	public void write(Put put) {
		counter.incrementAndGet();
		while (counter.get() > maxQueueLength) {
			;
		}

		queue.add(put);

		Batch batch = batchers.get(index);
		if (batch == null) {
			batchers.putIfAbsent(index, new Batch(BATCHER_NAME_PREFIX + index, htables[index - 1]));
			batch = batchers.get(index);
		}
		if ((index++) % htables.length == 0) {
			index = 1;
		}
		workerExecutor.execute(batch);
	}

	public void await() {
		while (counter.get() > 0) {
			;
		}
	}

	public void await(long interval, Progressable progressable) {
		while (counter.get() == 0) {
			;
		}
		while (counter.get() > 0) {
			if (interval > 0) {
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
				}
			}
			if (progressable != null) {
				progressable.progress(lines.get(), queue.size());
			}
		}
	}

	public void flush() {
		for (Batch batch : batchers.values()) {
			if (!batch.isEmpty()) {
				try {
					batch.doFlush();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}

	public void close() {
		queue.clear();
		batchers.clear();
		for (HTableInterface ht : htables) {
			try {
				ht.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		}

		if (workerExecutor instanceof ExecutorService) {
			try {
				ExecutorService realExec = ((ExecutorService) workerExecutor);
				realExec.shutdown();

				int i = 0;
				while (!realExec.isTerminated() && i++ < 3) {
					((ExecutorService) workerExecutor).shutdown();
					try {
						Thread.sleep(500L);
					} catch (InterruptedException e) {
					}
				}
				if (!realExec.isTerminated()) {
					realExec.shutdownNow();
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	protected HTableInterface createHTable(Configuration configuration, String tableName) throws IOException {
		return new HTable(configuration, tableName);
	}

*/}
