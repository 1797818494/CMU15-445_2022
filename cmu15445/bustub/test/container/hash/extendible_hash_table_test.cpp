/**
 * extendible_hash_test.cpp
 */

#include <memory>
#include <thread>  // NOLINT

#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"

namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

TEST(ExtendibleHashTableTest, GetNumBuckets) {
  auto table = std::make_unique<ExtendibleHashTable<int, int>>(4);

  std::vector<int> val;
  for (int i = 0; i <= 100; i++) {
    val.push_back(i);
  }

  // Inserting 4 keys belong to the same bucket
  table->Insert(4, val[4]);
  table->Insert(12, val[12]);
  table->Insert(16, val[16]);
  table->Insert(64, val[64]);
  ASSERT_EQ(1, table->GetNumBuckets());
  // Inserting into another bucket

  table->Insert(31, val[31]);
  ASSERT_EQ(2, table->GetNumBuckets());

  // Inserting into filled bucket 0
  table->Insert(10, val[10]);
  ASSERT_EQ(3, table->GetNumBuckets());

  // Inserting 3 keys into buckets with space
  table->Insert(51, val[51]);
  table->Insert(15, val[15]);
  table->Insert(18, val[18]);
  ASSERT_EQ(3, table->GetNumBuckets());

  // Inserting into filled buckets with local depth = global depth
  table->Insert(20, val[20]);
  ASSERT_EQ(4, table->GetNumBuckets());

  // Inserting 2 keys into filled buckets with local depth < global depth
  // Adding a new bucket and inserting will still be full so
  // will test if they add another bucket again.
  table->Insert(7, val[7]);
  table->Insert(23, val[21]);
  ASSERT_EQ(6, table->GetNumBuckets());

  // More Insertions(2 keys)
  table->Insert(11, val[11]);
  table->Insert(19, val[19]);
  ASSERT_EQ(6, table->GetNumBuckets());
}

}  // namespace bustub
