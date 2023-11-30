#include <gtest/gtest.h>
#include "engine/dummyJoin.h"

TEST(dummyJoin, firstTest) {
    dummyJoin dj = dummyJoin();
    ASSERT_EQ(dj.getResultWidth(), 2);
}