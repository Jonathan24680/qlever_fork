#include "dummyJoin.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "global/ValueId.h"
#include "ValuesForTesting.h"

// soll eine Klasse sein, welche aus dem nichts ein Ergebnis erzeugt

// note that this class is taking some unnecessary parameters for testing
dummyJoin::dummyJoin(QueryExecutionContext* qec,
            std::shared_ptr<QueryExecutionTree> t1,
            std::shared_ptr<QueryExecutionTree> t2,
            ColumnIndex t1JoinCol, ColumnIndex t2JoinCol,
            bool keepJoinColumn,
            IdTable leftChildTable,
            std::vector<std::optional<Variable>> variablesLeft,
            IdTable rightChildTable,
            std::vector<std::optional<Variable>> variablesRight)
        : Operation(qec) {
    /* ValuesForTesting operationLeft = ValuesForTesting(qec,
        std::move(leftChildTable), std::move(variablesLeft));
    ValuesForTesting operationRight = ValuesForTesting(qec,
        std::move(rightChildTable), std::move(variablesRight)); */
    _left = ad_utility::makeExecutionTree<ValuesForTesting>(qec,
        std::move(leftChildTable),std::move(variablesLeft));
    _right = ad_utility::makeExecutionTree<ValuesForTesting>(qec,
        std::move(rightChildTable), std::move(variablesRight));
    _leftJoinCol = t1JoinCol;
    _rightJoinCol = t2JoinCol;
    _keepJoinColumn = keepJoinColumn;
    _variablesLeft = std::move(variablesLeft);
    _variablesRight = std::move(variablesRight);
    _verbose_init = true;
}

std::vector<QueryExecutionTree*> dummyJoin::getChildren() {

}


string dummyJoin::asStringImpl(size_t indent) const {
    std::string dummy = "Hallo";
    return dummy;
}

string dummyJoin::getDescriptor() const {
    std::string dummy = "Hallo";
    return dummy;
}

size_t dummyJoin::getResultWidth() const { // muss angepasst werden
    if (_verbose_init) {
        return _variablesLeft.size() + _variablesRight.size() - 1;
    } else {
        return 1;
    }
}

size_t dummyJoin::getCostEstimate() {
    return 1;
}

uint64_t dummyJoin::getSizeEstimateBeforeLimit() {
    return 1;
}

float dummyJoin::getMultiplicity(size_t col) {
    return 1;
}

bool dummyJoin::knownEmptyResult() {
    return false;
}

vector<ColumnIndex> dummyJoin::resultSortedOn() const {
    return {};
}

ResultTable dummyJoin::computeResult() { // muss angepasst werden
    if (_verbose_init) {
        // compute the result based on the two child results
        // this assumes, that the IDtables are already sorted on the join column
    } else {
        // soll einfach nur eine random result table erstellen, welche alles
        // enthält, sodass es mit resultwidth usw. passt
        // return {};
        // die resulttable kann ein leeres localvocab haben
        IdTable idtable = IdTable(1, _allocator);
        for (int i = 0; i < 10; i++) {
            ValueId toAdd = ValueId::makeFromInt(i);
            idtable.push_back({toAdd});
        }
        LocalVocab lv = {}; // let's assume, this has no local vocabs
        std::vector<ColumnIndex> sortedBy = {0};
        return ResultTable(std::move(idtable), {}, std::move(lv));
    }
}

VariableToColumnMap dummyJoin::computeVariableToColumnMap() const {
    // welche Variable kommt zu welcher Spalte
    return {};
}