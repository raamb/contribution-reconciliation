from repository import Repository
from decimal import Decimal
import csv

AGIX_DECIMALS = 100000000

class AirdropReconciler:
    def __init__(self, ):
        self.__repository = Repository()
        self.__snapshots_to_reconcile = {}
        self.__sql_file = None

    def __write_file(self, statement):
        if not self.__sql_file:
            self.__sql_file = open("statements.sql", "w")
        self.__sql_file.write(statement)
        self.__sql_file.write("\n")

    def __populate_context(self):
        snapshot_query = "select snapshot_guid from user_balance_snapshot where airdrop_window_id = %s GROUP by snapshot_guid"
        result = self.__repository.execute(snapshot_query, [self.__window_id])
        for row in result:
            self.__snapshots_to_reconcile[row['snapshot_guid']] = row['snapshot_guid']
        print(self.__snapshots_to_reconcile)

    def __process_user(self, address, additional_balance):
        additional_balance = round(additional_balance, 0)
        print(f"Processing {address} with value {additional_balance}")
        user_balance_query = "select snapshot_guid, address, balance, total from user_balance_snapshot where airdrop_window_id = %s and address = %s"
        result = self.__repository.execute(user_balance_query, [self.__window_id, address])
        print(result)

        update_statement = "update user_balance_snapshot set balance = balance + {balance}, total = total + {balance}, " +\
        "row_updated = CURRENT_TIMESTAMP where airdrop_window_id = {window_id} and address = '{address}' and snapshot_guid ='{snapshot_guid}';"
        insert_statement = "insert into user_balance_snapshot (row_created, row_updated, airdrop_window_id, address, " +\
        "balance, snapshot_guid, staked, total) VALUES(CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, " +\
        "{window_id},'{address}',{balance},'{snapshot_guid}',0,{balance});"

        snapshots_seen = []
        for row in result:
            if row['snapshot_guid'] in self.__snapshots_to_reconcile:
                #User has a balance and or stake so add to the user's total
                user_stmt = update_statement.format(balance=additional_balance, window_id=self.__window_id, address=address, snapshot_guid=row['snapshot_guid'])
                snapshots_seen.append(row['snapshot_guid'])
                self.__write_file(user_stmt)
            
        for snapshot_guid in self.__snapshots_to_reconcile:
            if snapshot_guid not in snapshots_seen:
                print("INSERTNG")
                user_stmt = insert_statement.format(balance=additional_balance, window_id=self.__window_id, address=address, snapshot_guid=snapshot_guid)
                self.__write_file(user_stmt)
        
    def __validate_header(self, header):
        is_valid = True
        if "From" not in header or "Value" not in header:
            print("Invalid file provided. From and Value not set")
            is_valid = False
        return is_valid

    def process_dynaset(self, additional_balances_csv, airdrop_id, window_id):
        self.__airdrop_id = airdrop_id
        self.__window_id = window_id
        self.__additional_balances_csv = additional_balances_csv
        self.__populate_context()

        rows = []
        with open(additional_balances_csv, 'r') as file:
            csvreader = csv.reader(file)
            header = next(csvreader)
            print(header)
            if not self.__validate_header(header):
                return
            
            from_index = header.index("From")
            value_index = header.index("Value")
            for row in csvreader:
                #print(f"Processing {row[from_index]} with value {row[value_index]}")
                self.__process_user(row[from_index], Decimal(row[value_index].replace(',','')) * AGIX_DECIMALS)



a = AirdropReconciler()
a.process_dynaset("AGIX_contributors.csv",1,5)