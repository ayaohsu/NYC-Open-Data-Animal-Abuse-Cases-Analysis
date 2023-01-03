Goals
- To familiarize the usage of data engineering tools
- To get insights of complaints from NYC kaporos and locations

Different complaints type categories in Kaporo period vs avg of the year
- animal abuse
- dead animals
- animal odor
- in kaporo region by coordiante

Pie chart of complaint types

Resolution rate by complaint type

Draw a map with this year's data


1. Soda API -> s3
2. s3 -> oracle db with spark
3. Run queries on Oracle DB and export to excel 

# Usage

spark-submit \
  --jars /usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar \
  src/main.py