FLUX_DIR + "records.xml"
| open-file
| decode-xml
| handle-generic-xml("row")
| morph("morph.xml")
| encode-csv(separator=";", noQuotes="true")
| print;