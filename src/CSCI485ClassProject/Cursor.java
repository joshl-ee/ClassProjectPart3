package CSCI485ClassProject;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.fdb.FDBKVPair;
import CSCI485ClassProject.models.*;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.utils.ComparisonUtils;
import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import java.sql.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static CSCI485ClassProject.RecordsTransformer.getPrimaryKeyValTuple;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE,
  }

  // used by predicate
  private boolean isPredicateEnabled = false;
  private String predicateAttributeName;
  private Record.Value predicateAttributeValue;
  private ComparisonOperator predicateOperator;

  // Table Schema Info
  private String tableName;
  private TableMetadata tableMetadata;

  private RecordsTransformer recordsTransformer;

  private boolean isInitialized = false;

  private boolean isInitializedToLast = false;

  private final Mode mode;

  private AsyncIterator<KeyValue> iterator = null;

  private Record currentRecord = null;

  private Transaction tx;

  private DirectorySubspace directorySubspace;
  private DirectorySubspace indexSubspace;
  private boolean isMoved = false;
  private FDBKVPair currentKVPair = null;
  private String attrName = null;
  private IndexType indexType;
  private boolean isUsingIndex = false;

  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx) {
    this.mode = mode;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
    this.tx = tx;
  }

  // Constructor when using index
  public Cursor(Mode mode, String tableName, TableMetadata tableMetadata, Transaction tx, String attrName) {
    this.mode = mode;
    this.tableName = tableName;
    this.tableMetadata = tableMetadata;
    this.tx = tx;
    this.attrName = attrName;
    isUsingIndex = true;
  }

  public void setTx(Transaction tx) {
    this.tx = tx;
  }

  public Transaction getTx() {
    return tx;
  }

  public void abort() {
    if (iterator != null) {
      iterator.cancel();
    }

    if (tx != null) {
      FDBHelper.abortTransaction(tx);
    }

    tx = null;
  }

  public void commit() {
    if (iterator != null) {
      iterator.cancel();
    }
    if (tx != null) {
      FDBHelper.commitTransaction(tx);
    }

    tx = null;
  }

  public final Mode getMode() {
    return mode;
  }

  public boolean isInitialized() {
    return isInitialized;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public void setTableMetadata(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public void enablePredicate(String attrName, Record.Value value, ComparisonOperator operator) {
    this.predicateAttributeName = attrName;
    this.predicateAttributeValue = value;
    this.predicateOperator = operator;
    this.isPredicateEnabled = true;
  }



  private Record moveToNextRecord(boolean isInitializing) {
    if (!isInitializing && !isInitialized) {
      return null;
    }

    if (isInitializing) {
      // initialize the subspace and the iterator
      recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
      directorySubspace = FDBHelper.openSubspace(tx, recordsTransformer.getTableRecordPath());
      AsyncIterable<KeyValue> fdbIterable = FDBHelper.getKVPairIterableOfDirectory(directorySubspace, tx, isInitializedToLast);
      if (fdbIterable != null)
        iterator = fdbIterable.iterator();

      isInitialized = true;
    }
    // reset the currentRecord
    currentRecord = null;

    // no such directory, or no records under the directory
    if (directorySubspace == null || !hasNext()) {
      return null;
    }

    List<String> recordStorePath = recordsTransformer.getTableRecordPath();
    List<FDBKVPair> fdbkvPairs = new ArrayList<>();

    boolean isSavePK = false;
    Tuple pkValTuple = new Tuple();
    Tuple tempPkValTuple = null;
    if (isMoved && currentKVPair != null) {
      fdbkvPairs.add(currentKVPair);
      pkValTuple = getPrimaryKeyValTuple(currentKVPair.getKey());
      isSavePK = true;
    }

    isMoved = true;
    boolean nextExists = false;

    while (iterator.hasNext()) {
      KeyValue kv = iterator.next();
      Tuple keyTuple = directorySubspace.unpack(kv.getKey());
      Tuple valTuple = Tuple.fromBytes(kv.getValue());
      FDBKVPair kvPair = new FDBKVPair(recordStorePath, keyTuple, valTuple);
      tempPkValTuple = getPrimaryKeyValTuple(keyTuple);
      if (!isSavePK) {
        pkValTuple = tempPkValTuple;
        isSavePK = true;
      } else if (!pkValTuple.equals(tempPkValTuple)){
        // when pkVal change, stop there
        currentKVPair = kvPair;
        nextExists = true;
        break;
      }
      fdbkvPairs.add(kvPair);
    }
    if (!fdbkvPairs.isEmpty()) {
      currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
    }

    if (!nextExists) {
      currentKVPair = null;
    }
    return currentRecord;
  }

  private AsyncIterable<KeyValue> indexInitialize() {
    //System.out.println("initializing");
    // Set pointer to main data directory
    recordsTransformer = new RecordsTransformer(getTableName(), getTableMetadata());
    List<String> tablePath = recordsTransformer.getTableRecordPath();
    directorySubspace = FDBHelper.openSubspace(getTx(), tablePath);

    tablePath.set(tablePath.size()-1, attrName);
    // Check if bplus or index
    tablePath.add("bplus");
    if (FDBHelper.doesSubdirectoryExists(tx, tablePath)) {
      indexType = IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX;
    //  System.out.println("It is bplus");
    }
    else  {
      indexType = IndexType.NON_CLUSTERED_HASH_INDEX;
      tablePath.set(tablePath.size()-1, "hash");
    }

    // Set pointer to index structure directory
    indexSubspace = FDBHelper.openSubspace(getTx(), tablePath);

    // Set iterable object
    AsyncIterable<KeyValue> fdbIterable;
    Tuple rangeTuple = new Tuple();
    // If predicate mode is equalsTo, iterator must start pointing at attrValue
    if (predicateOperator == ComparisonOperator.EQUAL_TO) {
      if (indexType == IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX) rangeTuple = rangeTuple.addObject(predicateAttributeValue.getValue());
      else rangeTuple = rangeTuple.addObject(predicateAttributeValue.hashCode());
      fdbIterable = FDBHelper.getKVPairIterableStartWithPrefixInDirectory(indexSubspace, tx, rangeTuple, isInitializedToLast);
    }
    else {
      Range dirRange = indexSubspace.range(rangeTuple);
      fdbIterable = tx.getRange(dirRange, ReadTransaction.ROW_LIMIT_UNLIMITED, isInitializedToLast);
    }
    return fdbIterable;
  }
  private Record moveToNextRecordIndex(boolean isInitializing) {
    if (!isInitializing && !isInitialized) {
      return null;
    }

    // Initialize
    if (isInitializing) {
      AsyncIterable<KeyValue> fdbIterable = indexInitialize();
      if (fdbIterable != null) {
        iterator = fdbIterable.iterator();
        isInitialized = true;
        //System.out.println("here");
        //System.out.println("Iterator has next: " + iterator.hasNext());
      }
    }

    // reset the currentRecord
    currentRecord = null;

    // no such directory, or no records under the directory
    if (indexSubspace == null || !hasNext()) {
      //System.out.println("Here?");
      return null;
    }

    List<String> recordStorePath = recordsTransformer.getTableRecordPath();
    List<FDBKVPair> fdbkvPairs = new ArrayList<>();

    Tuple pkValTuple;
    AsyncIterator<KeyValue> newIterator = null;

    // Get records from main data using index structure
    if (iterator.hasNext()) {
      KeyValue kv = iterator.next();
      Tuple keyTuple = indexSubspace.unpack(kv.getKey());

      // Get primary key
      pkValTuple = keyTuple.popFront();
      //System.out.println("Looking for PK " + pkValTuple.get(0).toString() + " in main");
      // TODO: If pass comparison then just stop
      newIterator = FDBHelper.getKVPairIterableWithPrefixInDirectory(directorySubspace, tx, pkValTuple, isInitializedToLast).iterator();
    }

    while (newIterator != null && newIterator.hasNext()) {
      KeyValue kv = newIterator.next();
      Tuple keyTuple = directorySubspace.unpack(kv.getKey());
      Tuple valTuple = Tuple.fromBytes(kv.getValue());
      FDBKVPair kvPair = new FDBKVPair(recordStorePath, keyTuple, valTuple);
      fdbkvPairs.add(kvPair);
    }

    if (!fdbkvPairs.isEmpty()) {
      currentRecord = recordsTransformer.convertBackToRecord(fdbkvPairs);
    }

//    System.out.println("PK: " + currentRecord.getMapAttrNameToValue().get("SSN").getValue());
//    System.out.println("Salary: " + currentRecord.getMapAttrNameToValue().get("Salary").getValue());
    return currentRecord;
  }

  public Record getFirst() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = false;

    Record record;
    if (isUsingIndex)  record = moveToNextRecordIndex(true);
    else record = moveToNextRecord(true);

    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        if (isUsingIndex)  record = moveToNextRecordIndex(false);
        else record = moveToNextRecord(false);
      }
    }
    return record;
  }

  private boolean doesRecordMatchPredicate(Record record) {
    Object recVal = record.getValueForGivenAttrName(predicateAttributeName);
    AttributeType recType = record.getTypeForGivenAttrName(predicateAttributeName);
    if (recVal == null || recType == null) {
      // attribute not exists in this record
      return false;
    }

    if (recType == AttributeType.INT) {
      return ComparisonUtils.compareTwoINT(recVal, predicateAttributeValue.getValue(), predicateOperator);
    } else if (recType == AttributeType.DOUBLE){
      return ComparisonUtils.compareTwoDOUBLE(recVal, predicateAttributeValue.getValue(), predicateOperator);
    } else if (recType == AttributeType.VARCHAR) {
      return ComparisonUtils.compareTwoVARCHAR(recVal, predicateAttributeValue.getValue(), predicateOperator);
    }

    return false;
  }

  public Record getLast() {
    if (isInitialized) {
      return null;
    }
    isInitializedToLast = true;

    Record record;
    if (isUsingIndex)  record = moveToNextRecordIndex(true);
    else record = moveToNextRecord(true);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        if (isUsingIndex)  record = moveToNextRecordIndex(false);
        else record = moveToNextRecord(false);
      }
    }
    return record;
  }

  public boolean hasNext() {
    return isInitialized && iterator != null && (iterator.hasNext() || currentKVPair != null);
  }

  public Record next(boolean isGetPrevious) {
    if (!isInitialized) {
      return null;
    }
    if (isGetPrevious != isInitializedToLast) {
      return null;
    }

    Record record;
    if (attrName != null)  record = moveToNextRecordIndex(false);
    else record = moveToNextRecord(false);
    if (isPredicateEnabled) {
      while (record != null && !doesRecordMatchPredicate(record)) {
        if (attrName != null)  record = moveToNextRecordIndex(false);
        else record = moveToNextRecord(false);
      }
    }
    return record;
  }

  public Record getCurrentRecord() {
    return currentRecord;
  }

  public StatusCode updateCurrentRecord(String[] attrNames, Object[] attrValues) {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    Set<String> currentAttrNames = currentRecord.getMapAttrNameToValue().keySet();
    Set<String> primaryKeys = new HashSet<>(tableMetadata.getPrimaryKeys());

    boolean isUpdatingPK = false;
    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];

      if (!currentAttrNames.contains(attrNameToUpdate)) {
        return StatusCode.CURSOR_UPDATE_ATTRIBUTE_NOT_FOUND;
      }

      if (!Record.Value.isTypeSupported(attrValToUpdate)) {
        return StatusCode.ATTRIBUTE_TYPE_NOT_SUPPORTED;
      }

      if (!isUpdatingPK && primaryKeys.contains(attrNameToUpdate)) {
        isUpdatingPK = true;
      }
    }

    if (isUpdatingPK) {
      // delete the old record first
      StatusCode deleteStatus = deleteCurrentRecord();
      if (deleteStatus != StatusCode.SUCCESS) {
        return deleteStatus;
      }
    }

    for (int i = 0; i<attrNames.length; i++) {
      String attrNameToUpdate = attrNames[i];
      Object attrValToUpdate = attrValues[i];
      currentRecord.setAttrNameAndValue(attrNameToUpdate, attrValToUpdate);
    }

    List<FDBKVPair> kvPairsToUpdate = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToUpdate) {
      FDBHelper.setFDBKVPair(directorySubspace, tx, kv);
    }
    return StatusCode.SUCCESS;
  }

  public StatusCode deleteCurrentRecord() {
    if (tx == null) {
      return StatusCode.CURSOR_INVALID;
    }

    if (!isInitialized) {
      return StatusCode.CURSOR_NOT_INITIALIZED;
    }

    if (currentRecord == null) {
      return StatusCode.CURSOR_REACH_TO_EOF;
    }

    List<FDBKVPair> kvPairsToDelete = recordsTransformer.convertToFDBKVPairs(currentRecord);
    for (FDBKVPair kv : kvPairsToDelete) {
      FDBHelper.removeKeyValuePair(directorySubspace, tx, kv.getKey());
    }

    return StatusCode.SUCCESS;
  }
}