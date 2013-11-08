/*
 * run `mongo --quiet api-production dump_indexes.js` to gather all indexes on
 * a database
 */

curs = db.system.indexes.find();
print("[");
while (curs.hasNext()) {
        next = curs.next();
        trailing_str = "";
        if (curs.hasNext()) {
                trailing_str = ",";
        }
        print(tojson(next) + trailing_str);
}
print("]");
