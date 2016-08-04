package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;

/**
 * This class implements the default re-fetch schedule. That is, no matter if the page was changed or not, the <code>fetchInterval</code>
 * remains unchanged, and the updated page fetchTime will always be set to <code>fetchTime + fetchInterval * 1000</code>.
 * 
 * @author Andrzej Bialecki
 */
public class DbConfigFetchSchedule extends AbstractFetchSchedule {
	private float INC_RATE;

	private float DEC_RATE;

	private int MAX_INTERVAL;

	private int MIN_INTERVAL;

	private boolean SYNC_DELTA;

	private double SYNC_DELTA_RATE;

	public void setConf(Configuration conf) {
		super.setConf(conf);
		if (conf == null)
			return;
		INC_RATE = conf.getFloat("db.fetch.schedule.adaptive.inc_rate", 0.2f);
		DEC_RATE = conf.getFloat("db.fetch.schedule.adaptive.dec_rate", 0.2f);
		MIN_INTERVAL = conf.getInt("db.fetch.schedule.adaptive.min_interval", 60);
		MAX_INTERVAL = conf.getInt("db.fetch.schedule.adaptive.max_interval", SECONDS_PER_DAY * 365); // 1 year
		SYNC_DELTA = conf.getBoolean("db.fetch.schedule.adaptive.sync_delta", true);
		SYNC_DELTA_RATE = conf.getFloat("db.fetch.schedule.adaptive.sync_delta_rate", 0.2f);
	}

	@Override
	public void setPageGoneSchedule(String url, WebPage page, long prevFetchTime, long prevModifiedTime, long fetchTime) {
		int newFetchInterval = (int) (page.getFetchInterval() * 5 + 30 * FetchSchedule.SECONDS_PER_DAY);
		page.setFetchInterval(newFetchInterval);
		page.setFetchTime(fetchTime + newFetchInterval * 1000L);
		page.setRetriesSinceFetch(page.getRetriesSinceFetch() + 1);
	}

	@Override
	public void setPageRetrySchedule(String url, WebPage page, long prevFetchTime, long prevModifiedTime, long fetchTime) {
		page.setFetchInterval(0);
		page.setFetchTime(fetchTime + SECONDS_PER_DAY * 1000L);
		page.setRetriesSinceFetch(page.getRetriesSinceFetch() + 1);
	}

	@Override
	public void setFetchSchedule(String url, WebPage page, long prevFetchTime, long prevModifiedTime, long fetchTime, long modifiedTime,
			int state) {
		super.setFetchSchedule(url, page, prevFetchTime, prevModifiedTime, fetchTime, modifiedTime, state);
		// page.setFetchTime(fetchTime + page.getFetchInterval() * 1000L);
		if (modifiedTime <= 0)
			modifiedTime = fetchTime;
		long refTime = fetchTime;
		if (modifiedTime <= 0)
			modifiedTime = fetchTime;
		int interval = page.getFetchInterval();
		switch (state) {
		case FetchSchedule.STATUS_MODIFIED:
			interval *= (1.0f - DEC_RATE);
			break;
		case FetchSchedule.STATUS_NOTMODIFIED:
			interval *= (1.0f + INC_RATE);
			break;
		case FetchSchedule.STATUS_UNKNOWN:
			break;
		}
		page.setFetchInterval(interval);
		if (SYNC_DELTA) {
			// try to synchronize with the time of change
			// TODO: different from normal class (is delta in seconds)?
			int delta = (int) ((fetchTime - modifiedTime) / 1000L);
			if (delta > interval)
				interval = delta;
			refTime = fetchTime - Math.round(delta * SYNC_DELTA_RATE);
		}

		if (interval < MIN_INTERVAL)
			interval = MIN_INTERVAL;
		if (interval > MAX_INTERVAL)
			interval = MAX_INTERVAL;
		if (page.getParseStatus() != null) {
			ParseStatus pstatus = page.getParseStatus();
			if (pstatus.getMajorCode() != ParseStatusCodes.SUCCESS && pstatus.getMinorCode() != ParseStatusCodes.SUCCESS_REDIRECT) {
				interval += 30 * SECONDS_PER_DAY;
			}
		} else {
			interval = Integer.MAX_VALUE;
		}
		page.setFetchTime(refTime + interval * 1000L);
		page.setFetchInterval(interval);
	}

	@Override
	public void initializeSchedule(String url, WebPage page) {
		page.setFetchTime(0);
		page.setFetchInterval(0);
		page.setRetriesSinceFetch(0);
	}

	@Override
	public boolean shouldFetch(String url, WebPage page, long curTime) {
		// pages are never truly GONE - we have to check them from time to time.
		// pages with too long fetchInterval are adjusted so that they fit within
		// maximum fetchInterval (segment retention period).
		// 时间判断
		// return true;
		boolean res = NutchConstant.checkInterval(page.nodeConfig, page, curTime);
		if (res)
			GeneratorJob.LOG.info(url + " fetchInterval:" + page.getFetchInterval() + "  FetchTime:" + page.getFetchTime()
					+ " PrevFetchTime:" + page.getPrevFetchTime() + " ConfigUrl:" + page.getConfigUrl() + " res=" + res);
		else
			GeneratorJob.LOG.info(url + " fetchInterval:" + page.getFetchInterval() + "  FetchTime:" + page.getFetchTime()
					+ " PrevFetchTime:" + page.getPrevFetchTime() + " ConfigUrl:" + page.getConfigUrl() + " res=" + res);
		return res;
		// long fetchTime = page.getFetchTime();
		// if (fetchTime - curTime > maxInterval * 1000L) {
		// if (page.getFetchInterval() > maxInterval) {
		// page.setFetchInterval(Math.round(maxInterval * 0.9f));
		// }
		// page.setFetchTime(curTime);
		// }
		// return fetchTime <= curTime;
	}
}
