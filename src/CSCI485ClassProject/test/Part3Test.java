package CSCI485ClassProject.test;

import CSCI485ClassProject.Cursor;
import CSCI485ClassProject.Indexes;
import CSCI485ClassProject.IndexesImpl;
import CSCI485ClassProject.Records;
import CSCI485ClassProject.RecordsImpl;
import CSCI485ClassProject.StatusCode;
import CSCI485ClassProject.TableManager;
import CSCI485ClassProject.TableManagerImpl;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class Part3Test {

  public static String EmployeeTableName = "Employee";
  public static String SSN = "SSN";
  public static String Name = "Name";
  public static String Email = "Email";
  public static String Age = "Age";
  public static String Address = "Address";
  public static String Salary = "Salary";

  public static String[] EmployeeTableAttributeNames = new String[]{SSN, Name, Email, Age, Address, Salary};
  public static String[] EmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address, Salary};
  public static AttributeType[] EmployeeTableAttributeTypes =
      new AttributeType[]{AttributeType.INT, AttributeType.VARCHAR, AttributeType.VARCHAR, AttributeType.INT, AttributeType.VARCHAR, AttributeType.INT};

  public static String[] UpdatedEmployeeTableNonPKAttributeNames = new String[]{Name, Email, Age, Address, Salary};
  public static String[] EmployeeTablePKAttributes = new String[]{"SSN"};


  public static int initialNumberOfRecords = 100;
  public static int updatedNumberOfRecords = initialNumberOfRecords / 2;
  public static int randSeed = 30;

  private TableManager tableManager;
  private Records records;
  private Indexes indexes;

  private long getPerfRandNumber(Random generator) {
    long randomLong = generator.nextLong();

    if (randomLong < 0) {
      randomLong = -randomLong;
    }
    return randomLong;
  }

  private String getName(long i) {
    return "Name" + i;
  }

  private String getEmail(long i) {
    return "ABCDEFGH" + i + "@usc.edu";
  }

  private long getAge(long i) {
    return (i+25)%90;
  }

  private String getAddress(long i) {
    return "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + i;
  }

  private long getSalary(long i) {
    return i;
  }

  private Record getExpectedEmployeeRecord(long ssn) {
    Record rec = new Record();
    String name = getName(ssn);
    String email = getEmail(ssn);
    long age = getAge(ssn);
    String address = getAddress(ssn);
    long salary = getSalary(ssn);

    rec.setAttrNameAndValue(SSN, ssn);
    rec.setAttrNameAndValue(Name, name);
    rec.setAttrNameAndValue(Email, email);
    rec.setAttrNameAndValue(Age, age);
    rec.setAttrNameAndValue(Address, address);
    rec.setAttrNameAndValue(Salary, salary);

    return rec;
  }


  @Before
  public void init(){
    tableManager = new TableManagerImpl();
    records = new RecordsImpl();
    indexes = new IndexesImpl();
  }

  @After
  public void close() {
    tableManager.closeDatabase();
    records.closeDatabase();
    indexes.closeDatabase();
  }
  @Test
  public void unitTest1() {
    tableManager.dropAllTables();

    // create the Employee Table, verify that the table is created
    TableMetadata EmployeeTable = new TableMetadata(EmployeeTableAttributeNames, EmployeeTableAttributeTypes,
        EmployeeTablePKAttributes);
    assertEquals(StatusCode.SUCCESS, tableManager.createTable(EmployeeTableName,
        EmployeeTableAttributeNames, EmployeeTableAttributeTypes, EmployeeTablePKAttributes));
    HashMap<String, TableMetadata> tables = tableManager.listTables();
    assertEquals(1, tables.size());
    assertEquals(EmployeeTable, tables.get(EmployeeTableName));

    for (int i = 0; i<initialNumberOfRecords; i++) {
      long ssn = i;
      String name = getName(ssn);
      String email = getEmail(ssn);
      long age = getAge(ssn);
      String address = getAddress(ssn);
      long salary = getSalary(ssn);

      Object[] primaryKeyVal = new Object[] {ssn};
      Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address, salary};

      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, EmployeeTableNonPKAttributeNames, nonPrimaryKeyVal));
    }

    // verify that the insertion succeeds
    Cursor cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ);
    assertNotNull(cursor);

    // initialize the first record
    Record rec = records.getFirst(cursor);
    // verify the first record
    assertNotNull(rec);
    long ssn = 0;
    assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
    assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
    assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
    assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
    assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));
    ssn++;

    while (true) {
      rec = records.getNext(cursor);
      if (rec == null) {
        break;
      }
      assertEquals(ssn, rec.getValueForGivenAttrName(SSN));
      assertEquals(getName(ssn), rec.getValueForGivenAttrName(Name));
      assertEquals(getEmail(ssn), rec.getValueForGivenAttrName(Email));
      assertEquals(getAge(ssn), rec.getValueForGivenAttrName(Age));
      assertEquals(getAddress(ssn), rec.getValueForGivenAttrName(Address));
      assertEquals(getSalary(ssn), rec.getValueForGivenAttrName(Salary));
      ssn++;
    }

    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
    assertEquals(ssn, initialNumberOfRecords);

    // create the index
    assertEquals(StatusCode.SUCCESS, indexes.createIndex(EmployeeTableName, Email, IndexType.NON_CLUSTERED_HASH_INDEX));
    assertEquals(StatusCode.INDEX_ALREADY_EXISTS_ON_ATTRIBUTE, indexes.createIndex(EmployeeTableName, Email, IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX));

    System.out.println("Test1 passed!");
  }

  @Test
  public void unitTest2() {
    assertEquals(StatusCode.SUCCESS, indexes.createIndex(EmployeeTableName, Salary, IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX));
    Cursor cursor = records.openCursor(EmployeeTableName, Salary, 75, ComparisonOperator.LESS_THAN, Cursor.Mode.READ, true);

    boolean isCursorInitialized = false;
    for (int i = 0; i < 75; i++) {
      Record record;
      if (!isCursorInitialized) {
        record = records.getFirst(cursor);
        isCursorInitialized = true;
      } else {
        record = records.getNext(cursor);
      }

      long ssn = i;
      Record expectRecord = getExpectedEmployeeRecord(ssn);
      assertEquals(expectRecord, record);
    }

    assertNull(records.getNext(cursor));
    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));

    assertNull(records.openCursor(EmployeeTableName, Age, 40, ComparisonOperator.LESS_THAN_OR_EQUAL_TO, Cursor.Mode.READ, true));
    System.out.println("Test2 passed!");
  }
//
//
//  @Test
//  public void unitTest3() {
//    assertEquals(StatusCode.SUCCESS, indexes.createIndex(EmployeeTableName, SSN, IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX));
//
//    for (int i = 100; i < initialNumberOfRecords + updatedNumberOfRecords; i++) {
//      long ssn = i;
//      String name = getName(ssn);
//      String email = getEmail(ssn);
//      long age = getAge(ssn);
//      String address = getAddress(ssn);
//      long salary = getSalary(ssn);
//
//      Object[] primaryKeyVal = new Object[] {ssn};
//      Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address, salary};
//
//      assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, EmployeeTableNonPKAttributeNames, nonPrimaryKeyVal));
//    }
//
//    Cursor cursor = records.openCursor(EmployeeTableName, Salary, 100, ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, Cursor.Mode.READ, true);
//    assertNotNull(cursor);
//
//    boolean isCursorInitialized = false;
//    for (int i = 100; i < initialNumberOfRecords + updatedNumberOfRecords; i++) {
//      Record record;
//      if (!isCursorInitialized) {
//        record = records.getFirst(cursor);
//        isCursorInitialized = true;
//      } else {
//        record = records.getNext(cursor);
//      }
//
//      long ssn = i;
//      Record expectRecord = getExpectedEmployeeRecord(ssn);
//      assertEquals(expectRecord, record);
//    }
//
//    assertNull(records.getNext(cursor));
//    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
//    System.out.println("Test3 passed!");
//  }
//
//  @Test
//  public void unitTest4() {
//    Cursor cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ_WRITE);
//
//    boolean isCursorInitialized = false;
//    while (true) {
//      Record record;
//      if (!isCursorInitialized) {
//        record = records.getFirst(cursor);
//        isCursorInitialized = true;
//      } else {
//        record = records.getNext(cursor);
//      }
//
//      if (record == null) {
//        break;
//      }
//      long ssn = (long) record.getValueForGivenAttrName(SSN);
//      if (ssn % 2 == 1) {
//        // Odd SSN record, delete it
//        assertEquals(StatusCode.SUCCESS, records.deleteRecord(cursor));
//      }
//    }
//
//    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
//
//    cursor = records.openCursor(EmployeeTableName, SSN, initialNumberOfRecords + updatedNumberOfRecords, ComparisonOperator.LESS_THAN_OR_EQUAL_TO, Cursor.Mode.READ, true);
//    isCursorInitialized = false;
//    while (true) {
//      Record record;
//      if (!isCursorInitialized) {
//        record = records.getFirst(cursor);
//        isCursorInitialized = true;
//      } else {
//        record = records.getNext(cursor);
//      }
//
//      if (record == null) {
//        break;
//      }
//      long ssn = (long) record.getValueForGivenAttrName(SSN);
//      assertEquals(0, ssn % 2);
//    }
//
//    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
//    System.out.println("Test4 passed!");
//  }
//
//  @Test
//  public void unitTest5() {
//    for (int i = 0; i < initialNumberOfRecords + updatedNumberOfRecords; i++) {
//      if (i % 2 == 1) {
//        // insert the odd record back
//        long ssn = i;
//        String name = getName(ssn);
//        String email = getEmail(ssn);
//        long age = getAge(ssn);
//        String address = getAddress(ssn);
//        long salary = getSalary(ssn);
//
//        Object[] primaryKeyVal = new Object[] {ssn};
//        Object[] nonPrimaryKeyVal = new Object[] {name, email, age, address, salary};
//
//        assertEquals(StatusCode.SUCCESS, records.insertRecord(EmployeeTableName, EmployeeTablePKAttributes, primaryKeyVal, EmployeeTableNonPKAttributeNames, nonPrimaryKeyVal));
//      }
//    }
//
//    assertEquals(StatusCode.SUCCESS, indexes.dropIndex(EmployeeTableName, SSN));
//    System.out.println("Test5 passed!");
//  }
//
//  @Test
//  public void unitTest6() {
//    Cursor cursor = records.openCursor(EmployeeTableName, Cursor.Mode.READ_WRITE);
//
//    boolean isCursorInitialized = false;
//    for (int i = 0; i < initialNumberOfRecords + updatedNumberOfRecords; i++) {
//      Record record;
//      if (!isCursorInitialized) {
//        record = records.getFirst(cursor);
//        isCursorInitialized = true;
//      } else {
//        record = records.getNext(cursor);
//      }
//
//      long salary = (long) record.getValueForGivenAttrName(Salary);
//
//      assertEquals(StatusCode.SUCCESS, records.updateRecord(cursor, new String[] {Salary}, new Object[] {salary * 2}));
//    }
//
//    assertNull(records.getNext(cursor));
//    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
//    isCursorInitialized = false;
//
//    cursor = records.openCursor(EmployeeTableName, Salary, 0, ComparisonOperator.GREATER_THAN, Cursor.Mode.READ, true);
//    for (int i = 1; i < initialNumberOfRecords + updatedNumberOfRecords; i++) {
//      Record record;
//      if (!isCursorInitialized) {
//        record = records.getFirst(cursor);
//        isCursorInitialized = true;
//      } else {
//        record = records.getNext(cursor);
//      }
//      long salary = (long) record.getValueForGivenAttrName(Salary);
//      assertEquals(2 * getSalary(i), salary);
//    }
//    assertNull(records.getNext(cursor));
//    assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
//    System.out.println("Test6 passed!");
//  }
//
//  @Test
//  public void unitTest7() {
//    // perf test
//    // create the Perf Table
//
//    tableManager.dropAllTables();
//
//    int numOfRecords = 1000000;
//    int numOfQueries = 10000;
//
//    String INT0 = "INT0";
//    String INT1 = "INT1";
//    String INT2 = "INT2";
//
//    String[] PerfTableAttributeNames = new String[] {INT0, INT1, INT2};
//    AttributeType[] PerfTableAttributeTypes = new AttributeType[] {AttributeType.INT, AttributeType.INT, AttributeType.INT};
//    String[] PerfTablePKAttributes = new String[] {INT0};
//    String[] PerfTableNonPKAttributes = new String[] {INT1, INT2};
//
//    String PerfTableName = "Test7";
//    assertEquals(StatusCode.SUCCESS, tableManager.createTable(PerfTableName,
//        PerfTableAttributeNames, PerfTableAttributeTypes, PerfTablePKAttributes));
//
//    Random randGenerator = new Random(randSeed);
//    for (int i = 0; i < numOfRecords; i++) {
//      Long randVal = getPerfRandNumber(randGenerator);
//
//      Object[] primaryKeyVal = new Object[] {i};
//      Object[] nonPrimaryKeyVal = new Object[] {randVal, randVal};
//
//      assertEquals(StatusCode.SUCCESS, records.insertRecord(PerfTableName, PerfTablePKAttributes, primaryKeyVal, PerfTableNonPKAttributes, nonPrimaryKeyVal));
//    }
//
//    long startTime = System.nanoTime();
//    randGenerator = new Random(randSeed);
//    for (int i = 0; i < numOfQueries; i++) {
//      Long randVal = getPerfRandNumber(randGenerator);
//      Cursor cursor = records.openCursor(PerfTableName, INT1, randVal, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ, false);
//      assertNotNull(records.getFirst(cursor));
//      assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
//    }
//    long endTime = System.nanoTime();
//    long executionTimeWithoutIndex = (endTime - startTime) / 1000;
//    System.out.println("Query " + numOfQueries + " Records without index: " + executionTimeWithoutIndex + " milliseconds");
//
//
//    assertEquals(StatusCode.SUCCESS, indexes.createIndex(PerfTableName, INT1, IndexType.NON_CLUSTERED_HASH_INDEX));
//    startTime = System.nanoTime();
//    randGenerator = new Random(randSeed);
//    for (int i = 0; i < numOfQueries; i++) {
//      Long randVal = getPerfRandNumber(randGenerator);
//      Cursor cursor = records.openCursor(PerfTableName, INT1, randVal, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ, true);
//      assertNotNull(records.getFirst(cursor));
//      assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
//    }
//    endTime = System.nanoTime();
//    long executionTimeWithHashIndex = (endTime - startTime) / 1000;
//    System.out.println("Query " + numOfQueries + " Records with non-clustered hash index: " + executionTimeWithHashIndex + " milliseconds");
//
//
//    assertEquals(StatusCode.SUCCESS, indexes.dropIndex(PerfTableName, INT1));
//    assertEquals(StatusCode.SUCCESS, indexes.createIndex(PerfTableName, INT1, IndexType.NON_CLUSTERED_B_PLUS_TREE_INDEX));
//    startTime = System.nanoTime();
//    randGenerator = new Random(randSeed);
//    for (int i = 0; i < numOfQueries; i++) {
//      Long randVal = getPerfRandNumber(randGenerator);
//      Cursor cursor = records.openCursor(PerfTableName, INT1, randVal, ComparisonOperator.EQUAL_TO, Cursor.Mode.READ, true);
//      assertNotNull(records.getFirst(cursor));
//      assertEquals(StatusCode.SUCCESS, records.commitCursor(cursor));
//    }
//    endTime = System.nanoTime();
//    long executionTimeWithBPlusTreeIndex = (endTime - startTime) / 1000;
//    System.out.println("Query " + numOfQueries + " Records with non-clustered B+Tree index: " + executionTimeWithBPlusTreeIndex + " milliseconds");
//    System.out.println("Test7 passed!");
//  }
}