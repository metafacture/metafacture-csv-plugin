"csv.xml"
| open-file
| decode-xml
| handle-generic-xml("row")
| morph("morph.xml")
| encode-csv(separator=";", noQuotes="true", includeHeader="true")
| print;