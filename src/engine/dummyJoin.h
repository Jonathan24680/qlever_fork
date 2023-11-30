#pragma once

#include "Operation.h"

class dummyJoin : public Operation {
    public:
        virtual std::vector<QueryExecutionTree*> getChildren();
        // eindeutiger String für die Objekte der Klasse:
        virtual string asStringImpl(size_t indent = 0) const; 
        virtual string getDescriptor() const ; // für Menschen
        virtual size_t getResultWidth() const; // wie viele Variablen man zurückgibt
        virtual size_t getCostEstimate();
        virtual uint64_t getSizeEstimateBeforeLimit();
        virtual float getMultiplicity(size_t col);
        virtual bool knownEmptyResult();
        [[nodiscard]] virtual vector<ColumnIndex> resultSortedOn() const;
        virtual ResultTable computeResult();
        virtual VariableToColumnMap computeVariableToColumnMap() const;
};