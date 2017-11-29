import os, pandas, glob, sqlite3, csv, sys, time, argparse
import urllib2, boto3, json, pandas, time, os, sys, logging, argparse
from datetime import datetime
import uuid as myuuid
from botocore.exceptions import ClientError

__author__ = "Jiamao Zheng <jiamaoz@yahoo.com>"
__version__ = "Revision: 0.0.1"
__date__ = "Date: 2017-11-28"

# usage: python merging_sqlites_v6_new.py -m GTEx-V6p_New_HapMap-2017-11-29.sqlite -i v6p_new/ -o v6p_new/ -l v6p_new/

class SqliteDBMerged(object):
    def __init_(self):
        # logger 
        self.logger = ''

        # input path
        self.input_path = ''

        # output path
        self.output_path = ''

        # log path 
        self.log_path = ''

        # merged db name 
        self.merged_sqlite_db_name = ''

    # Logging function 
    def getLog(self):
        log_file_name = ''
        if self.log_path != '':
            if self.log_path[-1] != '/':
                self.log_path = self.log_path + '/'
        log_file_name = self.log_path + str(myuuid.uuid4()) + '.log'

        self.logger = logging.getLogger()
        fhandler = logging.FileHandler(filename=log_file_name, mode='w')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fhandler.setFormatter(formatter)
        self.logger.addHandler(fhandler)
        self.logger.setLevel(logging.INFO)

    # Funtion to get a pretty string for a given number of seconds.
    def timeString(self, seconds):
      tuple = time.gmtime(seconds);
      days = tuple[2] - 1;
      hours = tuple[3];
      mins = tuple[4];
      secs = tuple[5];
      if sum([days,hours,mins,secs]) == 0:
        return "<1s";
      else:
        string = str(days) + "d";
        string += ":" + str(hours) + "h";
        string += ":" + str(mins) + "m";
        string += ":" + str(secs) + "s";
      return string;

    # Get arguments 
    def get_args(self):
        # setup commond line arguments 
        parser = argparse.ArgumentParser()

        # bucket path 
        parser.add_argument('-m', '--merged_db_name', required=True, default='', type=str, help='e.g. gtex-v6p-1kg-2016-08-18.sqlite')

        # output path 
        parser.add_argument('-o', '--output_path', required=True, default='', type=str, help='a directory path you choosen to save merged sqlite db output')

        # log path 
        parser.add_argument('-l', '--log_path', required=True, default='', type=str, help='a directory path you choosen to store log')
        
        # input path 
        parser.add_argument('-i', '--input_path', required=True, default='', type=str, help='a directory path that hold all individual sqlite db')

        # parse the arguments 
        args = parser.parse_args()
        self.output_path = args.output_path.strip()
        self.log_path = args.log_path.strip()
        self.merged_db_name = args.merged_db_name.strip()
        self.input_path = args.input_path.strip()

        if self.output_path != '' and not os.path.exists(self.output_path):
            os.makedirs(self.output_path) 
            
        if self.log_path != '' and not os.path.exists(self.log_path):
            os.makedirs(self.log_path) 

        if self.output_path != '':
            if self.output_path[-1] != '/':
                self.output_path = self.output_path + '/'

        if self.input_path != '':
            if self.input_path[-1] != '/':
                self.input_path = self.input_path + '/'   
    # merge
    def merge(self):
        # create a new database
        predictdb_all = self.output_path + self.merged_db_name

        connection = sqlite3.connect(predictdb_all)
        ccc = connection.cursor() 

        # weights table (7 rows)
        ccc.execute("DROP TABLE IF EXISTS weights")
        ccc.execute("CREATE TABLE weights (gene text NOT NULL, rsid text NOT NULL, varID text NOT NULL, ref_allele text NULL, eff_allele text NULL, weight real NULL, tissue text NOT NULL, PRIMARY KEY(gene, rsid, tissue))")

        # extra table (26 rows)
        ccc.execute("DROP TABLE IF EXISTS extra")
        ccc.execute("CREATE TABLE extra ( gene text NOT NULL, genename text NOT NULL, gene_type text NULL, alpha real NULL, n_snps_in_window integer NULL, n_snps_in_model integer NULL, lambda_min_mse real NULL, test_R2_avg real NULL, test_R2_sd real NULL, cv_R2_avg real NULL, cv_R2_sd real NULL, in_sample_R2 real NULL, nested_cv_fisher_pval real NULL, rho_avg real NULL, rho_se real NULL, rho_zscore real NULL, pred_perf_R2 real NULL, pred_perf_pval real NULL, cv_rho_avg real NULL, cv_rho_se real NULL, cv_rho_avg_squared real NULL, cv_zscore_est real NULL, cv_zscore_pval real NULL, cv_pval_est real NULL, pred_perf_qval real NULL, tissue text NULL, PRIMARY KEY (gene, tissue))")

        # construction table 
        ccc.execute("DROP TABLE IF EXISTS construction")
        ccc.execute("CREATE TABLE construction (chromosome integer NOT NULL, cv_seed integer NOT NULL, tissue text NOT NULL, PRIMARY KEY(chromosome, tissue))")

        ccc.execute("DROP TABLE IF EXISTS sample_info")
        ccc.execute("CREATE TABLE sample_info (n_samples integer NOT NULL, population text NULL, tissue text NOT NULL, PRIMARY KEY(population, tissue))")
        
        # merge all sqlite databases into one sqlite database
        tableName = ['construction', 'extra', 'sample_info', 'weights']
        dbFileList = glob.glob(self.input_path + "*.db")

        database_names = []
        for dbFilename in dbFileList:
           database_names.append(dbFilename)

        for i in range(len(database_names)):
            print(database_names[i])
            conn = sqlite3.connect(database_names[i]) 
            c = conn.cursor() 

            tissue_name = database_names[i].split('.')[0][:-15]
            tissue_name = tissue_name.split('/')[len(tissue_name.split('/'))-1][9:]
            print(tissue_name)

            for table_name in tableName:
                output = ''
                try:  
                    c.execute("alter table '%s' " %table_name + ' add column tissue TEXT')
                    c.execute('update %s' %table_name + " set tissue = '%s' " %tissue_name)
                except Exception as e:
                    print(e)
                c.execute('select * from %s' %table_name)
                output = c.fetchall()

                # csv 
                csv_writer = ''
                if table_name == 'construction': 
                   csv_writer = csv.writer(open(self.output_path + self.merged_db_name.split('.')[0] + "_" + tissue_name + "_" + table_name + ".csv", "w"))
                   csv_writer.writerow(['chromosome', 'cv.seed', 'tissue'])
                elif table_name == 'extra':
                   csv_writer = csv.writer(open(self.output_path + self.merged_db_name.split('.')[0] + "_" + tissue_name + "_" + table_name + ".csv", "w"))
                   csv_writer.writerow(['gene', 'genename', 'gene_type', 'alpha', 'n_snps_in_window', 'n.snps.in.model', 'lambda_min_mse', 'test_R2_avg', 'test_R2_sd', 'cv_R2_avg', 'cv_R2_sd', 'in_sample_R2', 'nested_cv_fisher_pval', 'rho_avg', 'rho_se', 'rho_zscore', 'pred.perf.R2', 'pred.perf.pval', 'cv_rho_avg', 'cv_rho_se', 'cv_rho_avg_squared', 'cv_zscore_est', 'cv_zscore_pval', 'cv_pval_est', 'pred.perf.qval', 'tissue'])
                elif table_name == 'weights':
                   csv_writer = csv.writer(open(self.output_path + self.merged_db_name.split('.')[0] + "_" + tissue_name + "_" + table_name + ".csv", "w"))
                   csv_writer.writerow(['gene', 'rsid', 'varID', 'ref_allele', 'eff_allele', 'weight', 'tissue'])
                else:
                   csv_writer = csv.writer(open(self.output_path + self.merged_db_name.split('.')[0] + "_" + tissue_name + "_" + table_name + ".csv", "w"))
                   csv_writer.writerow(['n.samples', 'population', 'tissue'])
                csv_writer.writerows(output)

                # sqlite db 
                for row in output: 
                    if table_name == 'extra':
                        ccc.execute("insert into %s VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" %table_name, row) 
                    elif table_name == 'weights':
                        ccc.execute("insert into %s VALUES(?, ?, ?, ?, ?, ?, ?)" %table_name, row)
                    elif table_name == 'construction': 
                        ccc.execute("insert into %s VALUES(?, ?, ?)" %table_name, row)
                    else:
                        ccc.execute("insert into %s VALUES(?, ?, ?)" %table_name, row) 

            # commit and close db
            conn.commit()
            conn.close()

        # commit and close db
        connection.commit()
        connection.close() 

        # concat and output combined datasets
        merged_extra = glob.glob(self.output_path + '*extra.csv')
        merged_weights = glob.glob(self.output_path + '*weights.csv')
        merged_sample_info = glob.glob(self.output_path + '*sample_info.csv')
        merged_construction = glob.glob(self.output_path + '*construction.csv')

        for list in [merged_extra, merged_construction, merged_weights, merged_sample_info]:
            merged_final = ''
            merged = []

            for filename in list:
                merged.append(pandas.read_csv(filename))
                os.system('rm %s' %filename)
                print('remove %s' %filename)
            merged_final = pandas.concat(merged, axis=0)

            if 'extra' in list[0]:
                merged_final.to_csv(self.output_path + self.merged_db_name.split('.')[0] + "_" + 'extra_final.csv', index=None)
            elif 'weights' in list[0]:
                merged_final.to_csv(self.output_path + self.merged_db_name.split('.')[0] + "_" + 'weights_final.csv', index=None)
            elif 'construction' in list[0]:
                merged_final.to_csv(self.output_path + self.merged_db_name.split('.')[0] + "_" + 'construction_final.csv', index=None)
            else: 
                merged_final.to_csv(self.output_path + self.merged_db_name.split('.')[0] + "_" + 'sample_info_final.csv', index=None)



def main():
    # Instantial class
    start_time = time.time() 
    sqliteDBMerged = SqliteDBMerged()
    sqliteDBMerged.get_args()
    sqliteDBMerged.getLog()

    # merge  
    sqliteDBMerged.merge()

    msg = "\nElapsed Time: " + sqliteDBMerged.timeString(time.time() - start_time) # calculate how long the program is running
    sqliteDBMerged.logger.info(msg)
    print(msg) 

    msg = "\nDate: " + datetime.now().strftime('%Y-%m-%d') + "\n"
    sqliteDBMerged.logger.info(msg)
    print(msg)   

# INITIALIZE
if __name__ == '__main__':
    sys.exit(main())