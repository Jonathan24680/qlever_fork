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


shared_ptr<const ResultTable> geoJoinTest(dummyJoin* dj) {
  std::shared_ptr<ResultTable> res = std::make_shared<ResultTable>(
    ResultTable(IdTable(1, dj->_allocator),
          std::vector<ColumnIndex>{}, LocalVocab{}));
  return res;
}


void wkt_join_test() {

  std::ifstream ttlfile("/home/jonathan/Desktop/qlever/qlever-indices/"
                            "osm_liechtenstein/testIndexPrefixes2.ttl");
  std::stringstream buffer;
  buffer << ttlfile.rdbuf();
  // std::cerr << buffer.str() << std::endl;
  QueryExecutionContext* qec = ad_utility::testing::getQec(buffer.str());
  std::cout << "Num Triples of the index: "
              << qec->getIndex().numTriples().normal_ << std::endl;
  // dummy query for the testing of the new join method
  // load first bus stop
  TripleComponent a{Variable{"?a"}};
  TripleComponent geometry1{Variable{"?geometry"}};
  TripleComponent wkt1{Variable{"?wkt1"}};
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
  std::shared_ptr<QueryExecutionTree> scan3 =
      ad_utility::makeExecutionTree<IndexScan>(qec,
      Permutation::Enum::POS, SparqlTriple{a,
      "<http://www.opengis.net/ont/geosparql#hasGeometry>",
      geometry1});
  std::shared_ptr<QueryExecutionTree> scan4 =
      ad_utility::makeExecutionTree<IndexScan>(qec,
      Permutation::Enum::POS, SparqlTriple{geometry1,
      "<http://www.opengis.net/ont/geosparql#asWKT>",
      wkt1});
  std::shared_ptr<QueryExecutionTree> join1 =
      ad_utility::makeExecutionTree<Join>(qec, scan1, scan2, 0, 1, true);
  std::shared_ptr<QueryExecutionTree> join2 =
      ad_utility::makeExecutionTree<Join>(qec, join1, scan3, 1, 1, true);
  std::shared_ptr<QueryExecutionTree> join3 =
      ad_utility::makeExecutionTree<Join>(qec, join2, scan4, 2, 1, true);
  
  // load second bus stop
  TripleComponent b{Variable{"?b"}};
  TripleComponent geometry2{Variable{"?geometry2"}};
  TripleComponent wkt2{Variable{"?wkt2"}};
  std::shared_ptr<QueryExecutionTree> scan2_1 =
      ad_utility::makeExecutionTree<IndexScan>(qec,
      Permutation::Enum::POS, SparqlTriple{b,
          "<https://www.openstreetmap.org/wiki/Key:highway>",
          TripleComponent::Literal{
                RdfEscaping::normalizeRDFLiteral("\"bus_stop\"")}});
  std::shared_ptr<QueryExecutionTree> scan2_2 =
      ad_utility::makeExecutionTree<IndexScan>(qec,
      Permutation::Enum::POS, SparqlTriple{b,
      "<https://www.openstreetmap.org/wiki/Key:name>",
      Variable{"?name2"}});
  std::shared_ptr<QueryExecutionTree> scan2_3 =
      ad_utility::makeExecutionTree<IndexScan>(qec,
      Permutation::Enum::POS, SparqlTriple{b,
      "<http://www.opengis.net/ont/geosparql#hasGeometry>",
      geometry2});
  std::shared_ptr<QueryExecutionTree> scan2_4 =
      ad_utility::makeExecutionTree<IndexScan>(qec,
      Permutation::Enum::POS, SparqlTriple{geometry2,
      "<http://www.opengis.net/ont/geosparql#asWKT>",
      wkt2});
  std::shared_ptr<QueryExecutionTree> join2_1 =
      ad_utility::makeExecutionTree<Join>(qec, scan2_1, scan2_2, 0, 1, true);
  std::shared_ptr<QueryExecutionTree> join2_2 =
      ad_utility::makeExecutionTree<Join>(qec, join2_1, scan2_3, 1, 1, true);
  std::shared_ptr<QueryExecutionTree> join2_3 =
      ad_utility::makeExecutionTree<Join>(qec, join2_2, scan2_4, 2, 1, true);
  shared_ptr<const ResultTable> res1 = join3->getResult();
  shared_ptr<const ResultTable> res2 = join2_3->getResult();
  // for testing, if it worked
  /*Join joinT{qec, join2_2, scan2_4, 2, 1, true};
  std::shared_ptr<const ResultTable> restest = joinT.getResult();
  std::cout << "restest" << restest->size() << std::endl;
  print_table(qec, restest);*/

  // distance merge
  dummyJoin dj{qec, join3, join2_3, 0, 0, true};
  shared_ptr<const ResultTable> res = geoJoinTest(&dj);
}


void test_stuff() {
  std::cout << "Testing stuff in the test stuff method"
            << "=================================================" << std::endl;
  wkt_join_test();
  std::cout << "Done testing stuff in the test stuff method"
            << "=================================================" << std::endl;
}