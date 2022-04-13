#ifndef CATALOG_AGGTYP_H
#define CATALOG_AGGTYP_H

#define AGGTYPE_OTHER 0
#define AGGTYPE_COUNT 1
#define AGGTYPE_SUM 2
#define AGGTYPE_AVG 3
#define AGGTYPE_MIN 4
#define AGGTYPE_MAX 5

#define NUM_AGGTYPES 6

#define AGGTYPE(aggtype) CONCAT(AGGTYPE_, aggtype)

#endif  // CATALOG_AGGTYP_H
