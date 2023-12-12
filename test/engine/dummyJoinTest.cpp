#include <gtest/gtest.h>
#include "engine/dummyJoin.h"
#include "engine/ResultTable.h"
#include "engine/idTable/IdTable.h"
#include "util/MemorySize/MemorySize.h"
#include "util/AllocatorWithLimit.h"
#include "index/Index.h"
#include "parser/data/Variable.h"
#include "engine/QueryExecutionTree.h"

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

void fillIdTableWithData(IdTable* idt, int numRows, int numColumns,
        int skipNumbers) {
    for(int i = 0; i < numRows; i++) {
        idt->emplace_back();
        for(int k=0; k < numColumns; k++) {
            (*idt)[i][k] = ValueId::makeFromInt(i * skipNumbers + k * 1000);
        }
    }
}

TEST(dummyJoin, testHelper) {
    ad_utility::MemorySize ms = ad_utility::MemorySize::bytes(100000);
    ad_utility::AllocatorWithLimit<ValueId> allocator =
        ad_utility::makeAllocatorWithLimit<ValueId>({ms});
    IdTable idTable(2, allocator);
    fillIdTableWithData(&idTable, 5, 2, 2);
    ASSERT_EQ(idTable[0][0].getInt(), 0);
    ASSERT_EQ(idTable[0][1].getInt(), 1000);
    ASSERT_EQ(idTable[1][0].getInt(), 2);
    ASSERT_EQ(idTable[1][1].getInt(), 1002);
    ASSERT_EQ(idTable[2][0].getInt(), 4);
    ASSERT_EQ(idTable[2][1].getInt(), 1004);
    ASSERT_EQ(idTable[3][0].getInt(), 6);
    ASSERT_EQ(idTable[3][1].getInt(), 1006);
    ASSERT_EQ(idTable[4][0].getInt(), 8);
    ASSERT_EQ(idTable[4][1].getInt(), 1008);

    IdTable idTable2(2, allocator);
    fillIdTableWithData(&idTable2, 5, 2, 5);
    ASSERT_EQ(idTable2[0][0].getInt(), 0);
    ASSERT_EQ(idTable2[0][1].getInt(), 1000);
    ASSERT_EQ(idTable2[1][0].getInt(), 5);
    ASSERT_EQ(idTable2[1][1].getInt(), 1005);
    ASSERT_EQ(idTable2[2][0].getInt(), 10);
    ASSERT_EQ(idTable2[2][1].getInt(), 1010);
}

// test the dummyJoin Class with two child operations
TEST(dummyJoin, twoChildren) {
    //initialize object
    ad_utility::MemorySize ms = ad_utility::MemorySize::bytes(100000);
    ad_utility::AllocatorWithLimit<ValueId> allocator =
        ad_utility::makeAllocatorWithLimit<ValueId>({ms});
    QueryResultCache qrc;
    QueryExecutionContext qec = QueryExecutionContext(Index{allocator}, &qrc,
    allocator, SortPerformanceEstimator{});
    std::vector<std::optional<Variable>> variablesLeft{
        Variable{"?JoinVar"}, Variable{"?VarLeft_1"}};
    std::vector<std::optional<Variable>> variablesRight{
        Variable{"?JoinVar"}, Variable{"?VarRight_1"}};
    IdTable leftIdTable(variablesLeft.size(), allocator);
    IdTable rightIdTable(variablesRight.size(), allocator);
    fillIdTableWithData(&leftIdTable, 10, 2, 2);
    fillIdTableWithData(&rightIdTable, 10, 2, 4);
    dummyJoin dj(&qec, nullptr, nullptr, 0, 0, true,
        std::move(leftIdTable), std::move(variablesLeft),
        std::move(rightIdTable), std::move(variablesRight));
    
    // test object
    ASSERT_EQ(dj._variablesLeft.size(), 2);
    ResultTable rt = dj.computeResult();
    ASSERT_EQ(rt.width(), 3);
    ASSERT_EQ(rt.width(), dj.getResultWidth());
    ASSERT_EQ(rt.size(), 5);
    ASSERT_EQ(rt.idTable().at(0, 0).getInt(), 0);
    ASSERT_EQ(rt.idTable().at(0, 1).getInt(), 1000);
    ASSERT_EQ(rt.idTable().at(0, 2).getInt(), 1000);
    ASSERT_EQ(rt.idTable().at(1, 0).getInt(), 4);
    ASSERT_EQ(rt.idTable().at(1, 1).getInt(), 1004);
    ASSERT_EQ(rt.idTable().at(1, 2).getInt(), 1004);
    ASSERT_EQ(rt.idTable().at(2, 0).getInt(), 8);
    ASSERT_EQ(rt.idTable().at(2, 1).getInt(), 1008);
    ASSERT_EQ(rt.idTable().at(2, 2).getInt(), 1008);
    ASSERT_EQ(rt.idTable().at(3, 0).getInt(), 12);
    ASSERT_EQ(rt.idTable().at(3, 1).getInt(), 1012);
    ASSERT_EQ(rt.idTable().at(3, 2).getInt(), 1012);
    ASSERT_EQ(rt.idTable().at(4, 0).getInt(), 16);
    ASSERT_EQ(rt.idTable().at(4, 1).getInt(), 1016);
    ASSERT_EQ(rt.idTable().at(4, 2).getInt(), 1016);

    // if this would not be for learning, but a real test, the following cases
    // should be checked as well:
    // test one/two empty subtree(s)
    // test join with no matches
    // test join with keepJoinColumn false and true
    // test join with many other columns
    // test join with join column in the beginning, middle and end
}