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
#include "util/GeoSparqlHelpers.h"


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


/* computes a join, which joins the two tables and writes the distance between
the two objects in the join column. If max distance is bigger than 0, then only
entries, which are closer than the max Distance are in the table
*/
shared_ptr<const ResultTable> geoJoinDistanceTest(dummyJoin* dj,
                                                  double maxDistance = 0) {
  const IdTable* res_left = &dj->_left->getResult()->idTable();
  const IdTable* res_right = &dj->_right->getResult()->idTable();
  size_t numColumns = res_left->numColumns() + res_right->numColumns() - 1;
  // IdTable result{dj->getResultWidth(), dj->_allocator};
  IdTable result{numColumns, dj->_allocator};

  /* this lambda function copies elements from copyFrom
  * into the table res. It copies them into the row
  * row_ind_res and column column col_ind_res. If the column
  * is equal to the joinCol, the column is not copied, but
  * skipped
  */
  auto addColumns = [](IdTable* res,
                      const IdTable* copyFrom,
                      size_t row_ind_res,
                      size_t col_ind_res,
                      size_t row_ind_copy,
                      size_t joinCol) {
      size_t col = 0;
      while (col < copyFrom->numColumns()) {
          if (col != joinCol) {
              res->at(row_ind_res, col_ind_res) = 
                  (*copyFrom).at(row_ind_copy, col);
              col_ind_res += 1;
          }
          col += 1;
      }
      return col_ind_res;
  };

  /* returns everything between the first two quotes. If the string does not
  * contain two quotes, the string is returned as a whole
  */
  auto betweenQuotes = [] (std::string extractFrom) {
    size_t pos1 = extractFrom.find("\"", 0);
    size_t pos2 = extractFrom.find("\"", pos1 + 1);
    if (pos1 != std::string::npos && pos2 != std::string::npos) {
      return extractFrom.substr(pos1 + 1, pos2-pos1-1);
    } else {
      return extractFrom;
    }
  };

  // cartesian product with the distance between the two objects
  size_t resrow = 0;
  for (size_t rowLeft = 0; rowLeft < res_left->size(); rowLeft++) {
    for (size_t rowRight = 0; rowRight < res_right->size(); rowRight++) {
      size_t rescol = 0;
      std::string point1 = ExportQueryExecutionTrees::idToStringAndType(
              dj->getExecutionContext()->getIndex(),
              res_left->at(rowLeft, dj->_leftJoinCol), {}).value().first;
      std::string point2 = ExportQueryExecutionTrees::idToStringAndType(
              dj->getExecutionContext()->getIndex(),
              res_right->at(rowRight, dj->_rightJoinCol), {}).value().first;
      point1 = betweenQuotes(point1);
      point2 = betweenQuotes(point2);
      double dist = ad_utility::detail::wktDistImpl(point1, point2);
      if (maxDistance == 0 || dist < maxDistance) {
        result.emplace_back();
        // distance calculation
        result.at(resrow, rescol) = ValueId::makeFromDouble(dist);
        rescol += 1;
        // add other columns to result table
        rescol = addColumns(&result, res_left, resrow, rescol,
                            rowLeft, dj->_leftJoinCol);
        rescol = addColumns(&result, res_right, resrow, rescol,
                            rowRight, dj->_rightJoinCol);
        resrow += 1;
      }
    }
  }
  
  std::shared_ptr<ResultTable> res = std::make_shared<ResultTable>(
    ResultTable(std::move(result),
          std::vector<ColumnIndex>{}, LocalVocab{}));
  print_table(dj->getExecutionContext(), res);
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
      //TripleComponent::Literal{
      //          RdfEscaping::normalizeRDFLiteral("\"Schibenertor\"")}});
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
  shared_ptr<const ResultTable> res = geoJoinDistanceTest(&dj, 0.5);
}


void test_stuff() {
  std::cout << "Testing stuff in the test stuff method"
            << "=================================================" << std::endl;
  wkt_join_test();
  std::cout << "Done testing stuff in the test stuff method"
            << "=================================================" << std::endl;
}