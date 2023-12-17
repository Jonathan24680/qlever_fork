#include "engine/dummyJoin.h"
#include "engine/ResultTable.h"
#include "engine/idTable/IdTable.h"
#include "util/MemorySize/MemorySize.h"
#include "util/AllocatorWithLimit.h"
#include "index/Index.h"
#include "parser/data/Variable.h"
#include "engine/QueryExecutionTree.h"
#include "./../test/IndexTestHelpers.h"
#include <fstream>
#include "engine/IndexScan.h"
#include "engine/Join.h"
#include "engine/ExportQueryExecutionTrees.h"


void print_table(QueryExecutionContext* qec,
                  const std::shared_ptr<const ResultTable> table) {
  for (size_t i = 0; i < table->size(); i++) {
    for (size_t k = 0; k < table->width(); k++) {
      auto test = ExportQueryExecutionTrees::idToStringAndType(qec->getIndex(),
            table->idTable().at(i, k), {});
      std::cout << test.value().first << " ";
    }
    std::cout << std::endl;
  }
}


void test_stuff() {
  std::cout << "Testing stuff in the test stuff method"
            << "=================================================" << std::endl;

  std::ifstream ttlfile("/home/jonathan/Desktop/qlever/qlever-indices/"
                            "osm_liechtenstein/testIndexPrefixes2.ttl");
    std::stringstream buffer;
    buffer << ttlfile.rdbuf();
    // std::cerr << buffer.str() << std::endl;
    QueryExecutionContext* qec = ad_utility::testing::getQec(buffer.str());
    std::cout << "Num Triples of the index: "
                << qec->getIndex().numTriples().normal_ << std::endl;
    // dummy query for the testing of the new join method
    TripleComponent a{Variable{"?a"}};
    /*std::shared_ptr<QueryExecutionTree> test =
        ad_utility::makeExecutionTree<IndexScan>(qec,
        Permutation::Enum::POS, SparqlTriple{a,
            PropertyPath::fromVariable(Variable{"?b"}), Variable{"?c"}});
    std::shared_ptr<QueryExecutionTree> test2 =
        ad_utility::makeExecutionTree<IndexScan>(qec,
        Permutation::Enum::POS, SparqlTriple{a,
        "<https://www.openstreetmap.org/wiki/Key:highway>", Variable{"?c"}});*/
    std::shared_ptr<QueryExecutionTree> scan1 =
        ad_utility::makeExecutionTree<IndexScan>(qec,
        Permutation::Enum::POS, SparqlTriple{a,
            "<https://www.openstreetmap.org/wiki/Key:highway>",
            TripleComponent::Literal{
                  RdfEscaping::normalizeRDFLiteral("\"bus_stop\"")}});
    std::shared_ptr<QueryExecutionTree> scan2 =
        ad_utility::makeExecutionTree<IndexScan>(qec,
        Permutation::Enum::POS, SparqlTriple{a,
        "<https://www.openstreetmap.org/wiki/Key:name>",
        Variable{"?name"}});
    // std::shared_ptr<const ResultTable> restest = scan2->getResult();
    Join join{qec, scan1, scan2, 0, 1, true};
    std::shared_ptr<const ResultTable> restest = join.getResult();
    std::cout << "restest " << restest->size() << std::endl;
    print_table(qec, restest);
    /*std::cout << a1->getVariableAndInfoByColumnIndex(0).first._name <<std::endl;
    std::cout << a2->getVariableAndInfoByColumnIndex(1).first._name <<std::endl;
    Join join1{qec, a1, a2, 0, 1, true};
    ASSERT_EQ(join1.getDescriptor(), "Join on ?a");
    std::shared_ptr<const ResultTable> res1 = join1.getResult();
    std::cout << "Res1 size: " << res1->size() << std::endl;
    std::cout << res1->asDebugString() << std::endl;*/
    // load first bus_stop
    // load second bus_stop
    // distance merge


  std::cout << "Done testing stuff in the test stuff method"
            << "=================================================" << std::endl;
}