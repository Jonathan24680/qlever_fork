#include "dummyJoin.h"

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
    // soll einfach nur eine random result table erstellen, welche alles erstellt, sodass es
    // mit resultwidth usw. passt
    // return {};
}

VariableToColumnMap dummyJoin::computeVariableToColumnMap() const {
    // welche Variable kommt zu welcher Spalte
    return {};
}