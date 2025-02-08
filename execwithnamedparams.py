import sys, getopt
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("execwithnamedparams").getOrCreate()
    opts, args = getopt.getopt(sys.argv[1:], "t:i:o:")
    
    formato, infile, outdir = "", "", ""

    for opt, arg in opts:
        if opt == "-t":
            formato = arg
        elif opt == "-i":
            infile = arg
        elif opt == "-o":
            outdir = arg
            
    print("\n\n\n\n-t ", formato)
    print("-i ", infile)
    print("-o ", outdir)
    dados = spark.read.csv(infile, header=False, inferSchema=True)
    dados.write.format(formato).save(outdir)
        
    spark.stop()
    
#spark-submit execwithnamedparams.py -t parquet -i /home/guilherme/download/despachantes.csv -o /home/guilherme/testesparquet/