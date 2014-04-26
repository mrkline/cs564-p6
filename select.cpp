#include <cstdio>
#include <cstdlib>

#include "catalog.hpp"
#include "query.hpp"

// forward declaration
const Status ScanSelect(const string & result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result,
                       const int projCnt,
                       const attrInfo projNames[],
                       const attrInfo *attr,
                       const Operator op,
                       const char *attrValue)
{
	// Qu_Select sets up things and then calls ScanSelect to do the actual work
	cout << "Doing QU_Select " << endl;
	Status status;
	int i;
	AttrDesc attrDesc;
	AttrDesc* projAttrInfo = new AttrDesc[projCnt];
	
	int length = 0;
	// grab the records for all attributes in projNames
	for(i = 0;i < projCnt; i++) {
		// call getInfo to get each record and store in projAttrInfo
		status = attrCat-> getInfo(projNames[i].relName,
			projNames[i].attrName, projAttrInfo[i]);
		if(status != OK) {
			return status;
		}	
		// add length to current length
		length += projAttrInfo[i].attrLen;
	}
	// if attribute is null return all rows of the relation table
	if(attr == NULL) {
		status = ScanSelect(result, projCnt, projAttrInfo, NULL, op, NULL, length);
		if(status != OK) {
			return status;
		}
	}
	else {
		// get the attribute and store its record in attrDesc
		status = attrCat->getInfo(attr->relName, attr->attrName,attrDesc);
		if(status != OK ){
			return status;
		}
	}
	
	
	// If our attribute type is float or int we have to 
	// convert it to char before calling startScan
	int j;
	float f;
	if(attrDesc.attrType == INTEGER) {
		j = atoi(attrValue);
		attrValue = (char *)&j;
	}
	else if(attrDesc.attrType == FLOAT) {
		f = atof(attrValue);
		// cast the float to a char
		attrValue = (char *)&f;
	}
	else {
		
	}
	
	// now that attrValue is of the right type, call Scan Select
	status = ScanSelect(result, projCnt, projAttrInfo, &attrDesc, op, attrValue, length);
	if(status != OK) {
		return status;
	}
	// we have to delete projAttrInfo to clear memory
	delete projAttrInfo;
	return status;
	
	
	

}


const Status ScanSelect(const string & result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen)
{
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;
	Status status;
	Record outRec;
	
	InsertFileScan ifs(result, status);
	if(status != OK) {
		return status;
	}
	
	// allocate a data field
	char * data = (char *) malloc(reclen);
	if(data == NULL) {
		return INSUFMEM;
	}
	
	outRec.data = data;
	outRec.length = reclen;
	
	
	// initialize heapfilescan object
	HeapFileScan hfs(projNames -> relName, status);
	if(status != OK) {
		return status;
	}
	
	// if we don't have a filter
	if(filter == NULL ) {
		if((status = hfs.startScan(0, 0, STRING, NULL, op)) != OK ) {
			return status;
		}
	}
	else {
		// do the scan with the filter
		status = hfs.startScan(attrDesc->attrOffset,attrDesc->attrLen,(Datatype)attrDesc -> attrType,filter, op);
		if(status != OK){
			return status;
		}
	}
	
	Record rec; 
	RID rid;
	
	
	// do the scan
	while((status = hfs.scanNext(rid)) == OK) {
		// get the record from the scan
		status = hfs.getRecord(rec);
		if(status != OK) {
			return status;
		} 
		
		int offset = 0;
		for(int i = 0;i< projCnt; i++) {
			// copy the record data +  offset into our data
			memcpy(data+offset, (char *)rec.data+projNames[i].attrOffset, projNames[i].attrLen);
			offset += projNames[i].attrLen;
		}
		
		// add the rec to the ifs to save result
		status = ifs.insertRecord(outRec, rid);
		if(status != OK) {
			return status;
		}
	}
	
	if(status == FILEEOF) {
		status = OK;
	}
	return status;


}
