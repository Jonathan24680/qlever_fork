#include "dummyJoin.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "global/ValueId.h"

// soll eine Klasse sein, welche aus dem nichts ein Ergebnis erzeugt

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
    return 1;
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
    // soll einfach nur eine random result table erstellen, welche alles
    // enthält, sodass es mit resultwidth usw. passt
    // return {};
    // die resulttable kann ein leeres localvocab haben
    ad_utility::MemorySize limit = ad_utility::MemorySize::bytes(1024);
    ad_utility::AllocatorWithLimit<ValueId> allocator =
            ad_utility::makeAllocatorWithLimit<ValueId>(limit);
    IdTable idtable = IdTable(1, allocator);
    for (int i = 0; i < 10; i++) {
        ValueId toAdd = ValueId::makeFromInt(i);
        idtable.push_back({toAdd});
    }
    LocalVocab lv = {}; // let's assume, this result table has no local vocabs
    std::vector<ColumnIndex> sortedBy = {0};
    return ResultTable(std::move(idtable), {}, std::move(lv));
}

VariableToColumnMap dummyJoin::computeVariableToColumnMap() const {
    // welche Variable kommt zu welcher Spalte
    return {};
}