# Next Steps Guide

Currently, the project have an issue. Suppose user specify a folder path for the data. The project currently does not auto detect the hireachy level. 

Suppose user specified the hiearchy to be 3, but the folder has hiearchy of 0. e.g. all the jsonlfiles are in the root folder, instead of being in the child folders, then there is an error things will fail. 


I want to write a function called lint_hierarchy(hierarchy_level=None) in folderdb, with the following logic. 

for example: 
- If user specified hireachy to be 2
- The program should list all the jsonlfile, look at the filename (make sure it has 2 dots) and create the necessary child folders and move the jsonlfile to the right child folder, also update all the meta data, such as (.idx, config.meta, db.meta, h.meta etc). 

If the hierarchy_level=None, then should default to the value in h.meta, else default to 1