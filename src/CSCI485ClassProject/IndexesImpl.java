package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IndexesImpl implements Indexes{
  private final Database db;

  public IndexesImpl() {
    db = FDBHelper.initialization();
  }
  private TableMetadata getTableMetadataByTableName(Transaction tx, String tableName) {
    TableMetadataTransformer tblMetadataTransformer = new TableMetadataTransformer(tableName);
    List<FDBKVPair> kvPairs = FDBHelper.getAllKeyValuePairsOfSubdirectory(tx,
            tblMetadataTransformer.getTableAttributeStorePath());
    TableMetadata tblMetadata = tblMetadataTransformer.convertBackToTableMetadata(kvPairs);
    return tblMetadata;
  }

  @Override
  public StatusCode createIndex(String tableName, String attrName, IndexType indexType) {
    Transaction tx = db.createTransaction();

    // Create path for index structure
    List<String> indexPath = new ArrayList<>();
    indexPath.add(tableName);
    indexPath.add(attrName);

    // Check if index on attribute already exists
    if (FDBHelper.doesSubdirectoryExists(tx, indexPath)) {
      FDBHelper.abortTransaction(tx);
      return StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE;
    }

    // Create hash index
    if (indexType == IndexType.NON_CLUSTERED_HASH_INDEX) {
      //System.out.println("Creating Non clustered hash index");

      // Open cursor on main data to loop through it
      RecordsImpl records = new RecordsImpl();
      Cursor cursor = records.openCursor(tableName, Cursor.Mode.READ);
      Record currRecord = records.getFirst(cursor);
      boolean firstProcessed = false;

      while (cursor.hasNext() || !firstProcessed) {
        if (!firstProcessed) firstProcessed = true;
        //System.out.println("here");
        currRecord = records.getNext(cursor);

        // Get hashValue of record on attrName
        int hashValue = currRecord.getHashCodeForGivenAttrName(attrName);

        // Create the indexed record's key tuple. This is (hashValue, primaryKey0, primaryKey1..., primaryKeyN).
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.add(hashValue); // Used to have .add(indexType). I don't think this is necessary anymore.

        TableMetadata metadata = getTableMetadataByTableName(tx, tableName);
        List<String> pkNames = metadata.getPrimaryKeys();
        Collections.sort(pkNames);

        for (String pk : pkNames) {
          keyTuple = keyTuple.addObject(currRecord.getValueForGivenAttrName(pk));
        }

        // Create the indexed record's value tuple. This is ().
        Tuple valueTuple = new Tuple();

        // Upload to FDB. Add "hash" to path to specify index type
        //indexPath.add("hash");
        DirectorySubspace indexSubspace = FDBHelper.createOrOpenSubspace(tx, indexPath);
        //System.out.println("Made " + indexPath);
        FDBHelper.setFDBKVPair(indexSubspace, tx, new FDBKVPair(indexPath, keyTuple, valueTuple));
      }
    }
    // TODO: Create B+ tree index
    else {
      // Open cursor on main data to loop through it
      RecordsImpl records = new RecordsImpl();
      Cursor cursor = records.openCursor(tableName, Cursor.Mode.READ);
      Record currRecord = records.getFirst(cursor);
      boolean firstProcessed = false;

      while (cursor.hasNext() || !firstProcessed) {
        if (!firstProcessed) firstProcessed = true;
        //System.out.println("here");
        currRecord = records.getNext(cursor);

        // Get attrValue of record on attrName
        Object attrValue = currRecord.getValueForGivenAttrName(attrName);

        // Create the indexed record's key tuple. This is (attrValue, primaryKey0, primaryKey1..., primaryKeyN).
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.addObject(attrValue); // Used to have .add(indexType). I don't think this is necessary anymore.

        TableMetadata metadata = getTableMetadataByTableName(tx, tableName);
        List<String> pkNames = metadata.getPrimaryKeys();
        Collections.sort(pkNames);

        for (String pk : pkNames) {
          keyTuple = keyTuple.addObject(currRecord.getValueForGivenAttrName(pk));
        }

        // Create the indexed record's value tuple. This is ().
        Tuple valueTuple = new Tuple();

        // Upload to FDB. Add "bplus" to path to specify index type
        //indexPath.add("bplus");
        DirectorySubspace indexSubspace = FDBHelper.createOrOpenSubspace(tx, indexPath);
        //System.out.println("Made " + indexPath);
        FDBHelper.setFDBKVPair(indexSubspace, tx, new FDBKVPair(indexPath, keyTuple, valueTuple));
      }
    }

    FDBHelper.commitTransaction(tx);
    return StatusCode.SUCCESS;
  }

  @Override
  public StatusCode dropIndex(String tableName, String attrName) {
    // your code
    return null;
  }
  @Override
  public void closeDatabase() {
    FDBHelper.close(db);
  }

}
