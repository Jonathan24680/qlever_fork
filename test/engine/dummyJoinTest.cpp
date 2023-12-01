#include <gtest/gtest.h>
#include "engine/dummyJoin.h"
#include "engine/ResultTable.h"
#include "engine/idTable/IdTable.h"

TEST(dummyJoin, firstTest) {
    dummyJoin dj = dummyJoin();
    ASSERT_EQ(dj.getResultWidth(), 1);
}

TEST(dummyJoin, computeResult) {
    dummyJoin dj = dummyJoin();
    ResultTable rt = dj.computeResult();
    ASSERT_EQ(rt.width(), 1);
    ASSERT_EQ(rt.width(), dj.getResultWidth());
    ASSERT_EQ(rt.size(), 10);
    const IdTable& idt = rt.idTable();
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(idt[i][0].getInt(), i);
    }
}