/* Copyright (c) 2013, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <gtest/gtest.h>
#include <sys/types.h>

#include <atomic>
#include <cstdlib>
#include <vector>
#include <chrono>
#include <future>
#include <thread>

#include "my_systime.h"
#include "my_thread.h"

#include "mysql/psi/mysql_mutex.h"
#include "sql/log.h"
#include "sql/sql_class.h"
#include "unittest/gunit/test_utils.h"

namespace log_throttle_unittest {

using my_testing::Server_initializer;

int summary_count = 0;
char last_query[32];
ulonglong last_lock_time = 0;
ulonglong last_exec_time = 0;

static bool slow_logger(THD *, const char *query, size_t, bool, ulonglong lock_time,
                        ulonglong exec_time) {
  summary_count++;
  last_lock_time = lock_time;
  last_exec_time = exec_time;
  strcpy(last_query, query);
  return false;
}

class LogThrottleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    initializer.SetUp();
    mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
    summary_count = 0;
  }

  void TearDown() override {
    initializer.TearDown();
    mysql_mutex_destroy(&m_mutex);
  }

  THD *thd() { return initializer.thd(); }

  Server_initializer initializer;
  mysql_mutex_t m_mutex;
};

// Slow_log_throttles test cases starts from here.

/*
  Test basic functionality - throttling, eligibility, printing of summary of
                             Slow_log_throttle.
*/
TEST_F(LogThrottleTest, SlowLogBasic) {
  ulong threshold = 2;
  ulong window = 1000000;
  Slow_log_throttle throttle(&threshold, &m_mutex, window, slow_logger, "%lu");

  // Should not be throttled
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));

  // Should not be throttled, not eligible
  EXPECT_FALSE(throttle.log(thd(), false));

  // Flush and check that summary was not printed
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(0, summary_count);

  // Should be throttled
  EXPECT_TRUE(throttle.log(thd(), true));

  // Flush and check that summary was printed
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);

  // Flush and check that summary was not printed again
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);

  // A summary does not replenish the current window quota.
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(2, summary_count);
}

// Test changes to the threshold level of slow logger
TEST_F(LogThrottleTest, SlowLogThresholdChange) {
  ulong threshold = 2;
  ulong window = 1000000;
  Slow_log_throttle throttle(&threshold, &m_mutex, window, slow_logger, "%lu");

  // Reach threshold
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));

  // Flush and check that summary was not printed
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(0, summary_count);

  // Changing the rate must not invent suppressed events.
  EXPECT_FALSE(throttle.update_rate(thd(), 1));
  EXPECT_EQ(0, summary_count);
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));

  // A changed rate retires the old epoch and applies to the next query.
  EXPECT_TRUE(throttle.update_rate(thd(), 3));
  EXPECT_STREQ("1", last_query);
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);
}

// Test number of suppressed messages written by slow logger
TEST_F(LogThrottleTest, SlowLogSuppressCount) {
  ulong threshold = 1;
  ulong window = 1000000;
  Slow_log_throttle throttle(&threshold, &m_mutex, window, slow_logger, "%lu");

  // Suppress 3 events
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);
  EXPECT_STREQ("3", last_query);

  // The exhausted quota remains exhausted after flushing.
  EXPECT_FALSE(throttle.log(thd(), false));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), false));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(2, summary_count);
  EXPECT_STREQ("2", last_query);
}
// End of Slow_log_throttles test cases.


// Ineligible statements need not wait for accounting in another connection.
TEST_F(LogThrottleTest, IneligibleWithoutWindowBypassesMutex) {
  ulong threshold = 1;
  Slow_log_throttle throttle(&threshold, &m_mutex, 60000000, slow_logger, "%lu");
  mysql_mutex_lock(&m_mutex);
  auto result = std::async(std::launch::async, [&] {
    my_thread_init();
    const bool suppressed = throttle.log(thd(), false);
    my_thread_end();
    return suppressed;
  });
  const auto ready = result.wait_for(std::chrono::seconds(5));
  mysql_mutex_unlock(&m_mutex);  // Also release a regressed implementation.
  EXPECT_EQ(std::future_status::ready, ready);
  EXPECT_FALSE(result.get());
  EXPECT_EQ(0, summary_count);
}

TEST_F(LogThrottleTest, IneligibleWithinWindowBypassesMutex) {
  ulong threshold = 1;
  Slow_log_throttle throttle(&threshold, &m_mutex, 60000000, slow_logger, "%lu");
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  mysql_mutex_lock(&m_mutex);
  auto result = std::async(std::launch::async, [&] {
    my_thread_init();
    const bool suppressed = throttle.log(thd(), false);
    my_thread_end();
    return suppressed;
  });
  const auto ready = result.wait_for(std::chrono::seconds(5));
  mysql_mutex_unlock(&m_mutex);
  EXPECT_EQ(std::future_status::ready, ready);
  EXPECT_FALSE(result.get());
  EXPECT_EQ(0, summary_count);
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_STREQ("1", last_query);
}

TEST_F(LogThrottleTest, IneligibleClosesExpiredWindow) {
  ulong threshold = 1;
  Slow_log_throttle throttle(&threshold, &m_mutex, 100000, slow_logger, "%lu");
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  std::this_thread::sleep_for(std::chrono::milliseconds(110));
  EXPECT_FALSE(throttle.log(thd(), false));
  EXPECT_EQ(1, summary_count);
  EXPECT_STREQ("1", last_query);
  EXPECT_FALSE(throttle.log(thd(), false));
  EXPECT_EQ(1, summary_count);
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
}

// The old implementation subtracted the new rate from the old total count.
TEST_F(LogThrottleTest, RateChangesReportActualSuppression) {
  for (ulong new_rate : {7UL, 2UL, 0UL, 100UL}) {
    ulong threshold = 5;
    Slow_log_throttle throttle(&threshold, &m_mutex, 60000000, slow_logger, "%lu");
    for (int i = 0; i < 40; ++i) EXPECT_EQ(i >= 5, throttle.log(thd(), true));
    EXPECT_TRUE(throttle.update_rate(thd(), new_rate));
    EXPECT_STREQ("35", last_query);
    EXPECT_FALSE(throttle.flush(thd()));
    if (new_rate == 0) {
      for (int i = 0; i < 40; ++i) EXPECT_FALSE(throttle.log(thd(), true));
    } else {
      for (ulong i = 0; i < new_rate; ++i) EXPECT_FALSE(throttle.log(thd(), true));
      EXPECT_TRUE(throttle.log(thd(), true));
      EXPECT_TRUE(throttle.flush(thd()));
      EXPECT_STREQ("1", last_query);
    }
  }
}

TEST_F(LogThrottleTest, SameRatePreservesRemainingQuota) {
  ulong threshold = 3;
  Slow_log_throttle throttle(&threshold, &m_mutex, 60000000, slow_logger, "%lu");
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.update_rate(thd(), 3));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.update_rate(thd(), 3));
  EXPECT_STREQ("1", last_query);
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_STREQ("1", last_query);
}

TEST_F(LogThrottleTest, SummaryDrainsCountsAndTimesTogether) {
  ulong threshold = 1;
  Slow_log_throttle throttle(&threshold, &m_mutex, 60000000, slow_logger, "%lu");
  EXPECT_FALSE(throttle.log(thd(), true));
  thd()->start_utime = my_micro_time() - 10000000;
  thd()->pop_lock_usec(7);
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_STREQ("1", last_query);
  EXPECT_EQ(7U, last_lock_time);
  const ulonglong first_exec_time = last_exec_time;
  EXPECT_GE(first_exec_time, 10000000U);

  thd()->start_utime = my_micro_time();
  thd()->pop_lock_usec(3);
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_STREQ("1", last_query);
  EXPECT_EQ(3U, last_lock_time);
  EXPECT_LT(last_exec_time, first_exec_time);
  EXPECT_FALSE(throttle.flush(thd()));
}

TEST_F(LogThrottleTest, EnableAfterStartupAndDisable) {
  ulong threshold = 0;
  Slow_log_throttle throttle(&threshold, &m_mutex, 60000000, slow_logger, "%lu");
  EXPECT_FALSE(throttle.update_rate(nullptr, 2));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.update_rate(thd(), 0));
  EXPECT_STREQ("1", last_query);
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.update_rate(thd(), 1));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
}

static std::atomic<ulong> concurrent_reported{0};
static std::atomic<ulonglong> concurrent_lock_time{0};
static bool concurrent_logger(THD *, const char *query, size_t, bool,
                              ulonglong lock_time, ulonglong) {
  concurrent_reported.fetch_add(std::strtoul(query, nullptr, 10));
  concurrent_lock_time.fetch_add(lock_time);
  return false;
}

TEST_F(LogThrottleTest, ConcurrentSameRateDoesNotRefillQuota) {
  ulong threshold = 1000;
  Slow_log_throttle throttle(&threshold, &m_mutex, 60000000,
                            concurrent_logger, "%lu");
  concurrent_reported = 0;
  concurrent_lock_time = 0;
  std::atomic<ulong> admitted{0}, suppressed{0};
  std::atomic<bool> start{false};
  std::vector<std::thread> workers;
  for (int t = 0; t < 8; ++t) {
    workers.emplace_back([&] {
      my_thread_init();
      THD *worker = new THD(false);
      worker->store_globals();
      worker->start_utime = my_micro_time();
      worker->pop_lock_usec(1);
      while (!start.load()) std::this_thread::yield();
      for (int i = 0; i < 4000; ++i) {
        if (throttle.log(worker, true))
          ++suppressed;
        else
          ++admitted;
      }
      delete worker;
      my_thread_end();
    });
  }
  start = true;
  for (int i = 0; i < 500; ++i) {
    throttle.update_rate(thd(), 1000);
    throttle.flush(thd());
  }
  for (auto &worker : workers) worker.join();
  throttle.flush(thd());
  EXPECT_EQ(1000U, admitted.load());
  EXPECT_EQ(31000U, suppressed.load());
  EXPECT_EQ(suppressed.load(), concurrent_reported.load());
  EXPECT_EQ(suppressed.load(), concurrent_lock_time.load());
}

TEST_F(LogThrottleTest, ConcurrentChangedRatesConserveStatistics) {
  ulong threshold = 5;
  Slow_log_throttle throttle(&threshold, &m_mutex, 60000000,
                            concurrent_logger, "%lu");
  concurrent_reported = 0;
  concurrent_lock_time = 0;
  std::atomic<ulong> suppressed{0};
  std::atomic<bool> start{false};
  std::vector<std::thread> workers;
  for (int t = 0; t < 4; ++t) {
    workers.emplace_back([&] {
      my_thread_init();
      THD *worker = new THD(false);
      worker->store_globals();
      worker->start_utime = my_micro_time();
      worker->pop_lock_usec(1);
      while (!start.load()) std::this_thread::yield();
      for (int i = 0; i < 8000; ++i)
        if (throttle.log(worker, true)) ++suppressed;
      delete worker;
      my_thread_end();
    });
  }
  start = true;
  for (int i = 0; i < 500; ++i) throttle.update_rate(thd(), (i % 3) * 5);
  for (auto &worker : workers) worker.join();
  throttle.flush(thd());
  EXPECT_EQ(suppressed.load(), concurrent_reported.load());
  EXPECT_EQ(suppressed.load(), concurrent_lock_time.load());
}

}  // namespace log_throttle_unittest
