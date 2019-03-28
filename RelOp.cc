#include "RelOp.h"
#include "BigQ.h"
#include <pthread.h>
#include <iostream>
#include <string>
#include "BigQ.h"
#include "DBFile.h"
#include <map>
#include <vector>

// Helper function

void GetSumRec(Record &finalRec, Type resType, int intSum, double dblSum) {

	string rec;

	if (resType == Int) {

		rec = to_string(intSum);
		rec.append("|");

		Attribute IA = { "int", Int };
		Schema out_schema("out_sch", 1, &IA);
		finalRec.ComposeRecord(&out_schema, rec.c_str());
	}
	else if (resType == Double) {
		rec = to_string(dblSum);
		rec.append("|");

		Attribute DA = { "double", Double };
		Schema out_schema("out_sch", 1, &DA);
		finalRec.ComposeRecord(&out_schema, rec.c_str());
	}
}

// Select file implementation
void *SelectFile::ReadFromDBFile(void *args) {
	
	thread_utils *sf = (thread_utils *)args;
	ComparisonEngine ceng;
	Record *temp = new Record(); 
	Schema *testSchema = new Schema("catalog", "partsupp");

	sf->inFile.MoveFirst();

	
	// sf->selOperator.Print();

	while (sf->inFile.GetNext(*temp)) {

		if (ceng.Compare(temp, &sf->literal, &sf->selOperator)) {	
			//temp.Print(testSchema);
			sf->outPipe->Insert(temp);
		}
	}

	sf->outPipe->ShutDown();
}


void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	// selOp.Print();

	//thread_utils args = { inFile, &outPipe, selOp, literal };
	thread_utils *args = new thread_utils;

	args->inFile = inFile;
	args->outPipe = &outPipe;
	args->literal = literal;
	args->selOperator = selOp;

	pthread_create(&thread, NULL, ReadFromDBFile, (void *) args);
}

void SelectFile::WaitUntilDone () {

	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	runLength = runlen;
}

/*
	PROJECT
*/
struct ProjectUtil{
	Pipe *inPipe;
	Pipe *outPipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
	ProjectUtil(Pipe *i, Pipe *o, int *k, int nI, int nO){
		inPipe = i;
		outPipe = o;
		keepMe = k;
		numAttsInput = nI;
		numAttsOutput = nO;
	}
	~ProjectUtil();
};

void *projectWorker(void *args){
	ProjectUtil* pU = (ProjectUtil*)args;
	Record rec;
	int cnt = 0;
	Schema *testSchema = new Schema("catalog", "part");
	while (pU->inPipe->Remove (&rec)) {
		// rec.Print (testSchema);
		cnt++;
		rec.Project(pU->keepMe, pU->numAttsOutput, pU->numAttsInput);
		pU->outPipe->Insert(&rec);
	}
	cout << "Count is " << cnt;

	pU->outPipe->ShutDown();
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 
	Pipe* temp = &outPipe;
	cout << "Inside Project run";
	ProjectUtil* pU = new ProjectUtil(&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput);
	pthread_create(&thread, NULL, projectWorker, (void *)pU);
}

void Project::WaitUntilDone () {
	 pthread_join (thread, NULL);
}

void Project::Use_n_Pages(int n) {
	runLength = n;
}


/*
Select Pipe implementation
*/
void *SelectPipe::ReadFromPipe(void *args) {

	thread_utils *sf = (thread_utils *)args;
	ComparisonEngine ceng;
	Record *temp = new Record();
	Schema *testSchema = new Schema("catalog", "partsupp");

	sf->selOperator.Print();

	while (sf->inPipe->Remove(temp)) {

		if (ceng.Compare(temp, &sf->literal, &sf->selOperator)) {
			sf->outPipe->Insert(temp);
		}
	}

	sf->outPipe->ShutDown();
}


void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {

	selOp.Print();

	//thread_utils args = { inFile, &outPipe, selOp, literal };
	thread_utils *args = new thread_utils;

	args->inPipe = &inPipe;
	args->outPipe = &outPipe;
	args->literal = literal;
	args->selOperator = selOp;

	pthread_create(&thread, NULL, ReadFromPipe, (void *)args);
}

void SelectPipe::WaitUntilDone() {

	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {
	runLength = runlen;
}

// Sum implementation
void *Sum::ComputeSum(void *args)
{
	thread_utils *s = (thread_utils *)args;
	Schema *testSchema = new Schema("catalog", "supplier");
	Record *temp = new Record();
	int intSum = 0, intParam = 0;
	double dblSum = 0, dblParam = 0;
	Type resType;

	string rec;
	Record finalRec;

	while (s->inPipe->Remove(temp)) {

		resType = s->computeMe.Apply(*temp, intParam, dblParam);

		if (resType == Int)
			intSum += intParam;
		else if (resType == Double)
			dblSum += dblParam;
	}

	/*if (resType == Int) {

		rec = to_string(intSum);
		rec.append("|");

		Attribute IA = { "int", Int };
		Schema out_schema("out_sch", 1, &IA);
		finalRec.ComposeRecord(&out_schema, rec.c_str());
	}
	else if (resType == Double) {
		rec = to_string(dblSum);
		rec.append("|");

		Attribute DA = { "double", Double };
		Schema out_schema("out_sch", 1, &DA);
		finalRec.ComposeRecord(&out_schema, rec.c_str());
	}*/

	GetSumRec(finalRec, resType, intSum, dblSum);

	cout << "double sum = " << dblSum << endl;
	cout << "int sum = " << intSum << endl;

	s->outPipe->Insert(&finalRec);
	s->outPipe->ShutDown();

}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {

	thread_utils *args = new thread_utils;

	args->inPipe = &inPipe;
	args->outPipe = &outPipe;
	args->computeMe = computeMe;

	pthread_create(&thread, NULL, ComputeSum, (void *)args);
}

void Sum::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int runlen) {
	runLength = runlen;
}

// Duplicate Removal Implementation

void *DuplicateRemoval::DupRemovalThread(void *args) {

	DuplicateRemoval *dr = (DuplicateRemoval *)args;
	Schema *testSchema = new Schema("catalog", "partsupp");

	OrderMaker *sortorder = new OrderMaker(dr->mySchema);

	Pipe sortedPipe(100);
	BigQ bq(*dr->inPipe, sortedPipe, *sortorder, dr->runLength);
	ComparisonEngine ceng;

	int count = 0;

	Record *temp = new Record();
	Record *prev = new Record();
	Record *toBeInserted;

	cout << "Remove from sorted file" << endl;

	if (sortedPipe.Remove(prev)) {
		toBeInserted = new Record();
		toBeInserted->Copy(prev);
		dr->outPipe->Insert(toBeInserted);
	}

	while (sortedPipe.Remove(temp)) {
		
		if (ceng.Compare(temp, prev, sortorder) != 0) {
			
			Record *toBeInserted = new Record();
			toBeInserted->Copy(temp);			
			dr->outPipe->Insert(toBeInserted);
			count++;
		}	
		//temp.Print(testSchema);
		
		prev->Copy(temp);
	}

	cout << "Count of distinct recs = " << count << endl;

	delete temp;
	delete prev;
	delete toBeInserted;
	dr->outPipe->ShutDown();
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {

	//thread_utils *args = new thread_utils;
	cout << "inside duplicate removal" << endl;
	
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;

	pthread_create(&thread, NULL, DupRemovalThread, this);
}

void DuplicateRemoval::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen) {
	this->runLength = runlen;
}

// Writeout implementation

void *WriteOut::WriteToFile(void *args) {

	WriteOut *wo = (WriteOut *)args;
	Schema *testSchema = new Schema("catalog", "partsupp");
	int count = 0;
	Record *temp = new Record(), *toBeInserted;
	if (wo->outFile) {

		while (wo->inPipe->Remove(temp))
		{
			toBeInserted = new Record();
			toBeInserted->Copy(temp);
			toBeInserted->WriteRecord(wo->mySchema, wo->outFile);
			count++;
		}
	}

	cout << "count inserted in file " << count << endl;
	fclose(wo->outFile);
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {

	this->inPipe = &inPipe;
	this->mySchema = &mySchema;
	this->outFile = outFile;

	pthread_create(&thread, NULL, WriteToFile, this);

}

void WriteOut::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int runlen) {
	this->runLength = runlen;
}

void *GroupBy::GroupByThread(void *args) {

	GroupBy *gb = (GroupBy *)args;
	Record *temp = new Record(), *prev = new Record(), *toBeInserted;
	ComparisonEngine ceng;
	Schema *testSchema = new Schema("catalog", "partsupp");
	/*map<Record*, vector<Record*>> groupTable;
	map<Record*, vector<Record*>>::iterator it;*/	

	int intParam = 0, intSum = 0;
	double dblParam = 0, dblSum = 0;
	Type resType;
	Record finalRec;

	Pipe sortedPipe(100);
	BigQ *bq = new BigQ(*gb->inPipe, sortedPipe, *gb->groupAtts, gb->runLength);

	if (sortedPipe.Remove(prev)) {
		
		resType = gb->computeMe.Apply(*prev, intParam, dblParam);

		if (resType == Int)
			intSum += intParam;
		else if (resType == Double)
			dblSum += dblParam;
	}

	while (sortedPipe.Remove(temp)) {

		if (ceng.Compare(temp, prev, gb->groupAtts) != 0) {

			GetSumRec(finalRec, resType, intSum, dblSum);

			gb->outPipe->Insert(&finalRec);
			
			intSum = 0;
			dblSum = 0;
		}

		resType = gb->computeMe.Apply(*temp, intParam, dblParam);

		if (resType == Int)
			intSum += intParam;
		else if (resType == Double)
			dblSum += dblParam;
		

		prev->Copy(temp);
	}

	GetSumRec(finalRec, resType, intSum, dblSum);

	gb->outPipe->Insert(&finalRec);

	/*while (gb->inPipe->Remove(temp)) {
		
		for(it = groupTable.begin(); it != groupTable.end(); it++) {

			if (ceng.Compare(temp, it->first, gb->groupAtts) == 0) {

				keyPresent = true;
				it->second.push_back(temp);
			}
		}

		if (!keyPresent)
			groupTable.insert(pair<Record*, vector<Record*> >(temp, {}));
	}

	

	for (it = groupTable.begin(); it != groupTable.end(); it++) {

		resType = gb->computeMe.Apply(*it->first, intParam, dblParam);

		if (resType == Int)
			intSum += intParam;
		else if (resType == Double)
			dblSum += dblParam;

		int n = it->second.size();

		for (int i = 0; i < n; i++)
		{
			resType = gb->computeMe.Apply(*it->second[i], intParam, dblParam);

			if (resType == Int)
				intSum += intParam;
			else if (resType == Double)
				dblSum += dblParam;
		}

		if (resType == Int) {

			rec = to_string(intSum);
			rec.append("|");

			Attribute IA = { "int", Int };
			Schema out_schema("out_sch", 1, &IA);
			finalRec.ComposeRecord(&out_schema, rec.c_str());
		}
		else if (resType == Double) {
			rec = to_string(dblSum);
			rec.append("|");

			Attribute DA = { "double", Double };
			Schema out_schema("out_sch", 1, &DA);
			finalRec.ComposeRecord(&out_schema, rec.c_str());
		}

		gb->outPipe->Insert(&finalRec);
	}*/
	
	gb->outPipe->ShutDown();

}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {

	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = computeMe;

	pthread_create(&thread, NULL, GroupByThread, this);
}

void GroupBy::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
	this->runLength = runlen;
}

/*
	JOIN 
*/
struct JoinUtil{
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	JoinUtil(Pipe *iL, Pipe *iR, Pipe *o, CNF *s, Record *l){
		inPipeL = iL;
		inPipeR = iR;
		outPipe = o;
		selOp = s;
		literal = l;
	}
	~JoinUtil();
};

void getJoinAttsToKeep(int *attsToKeep, int numAttsL, int numAttsR){
	for(int i=0; i<numAttsL; i++){
		attsToKeep[i] = i;
	}
	for(int i=0; i<numAttsR; i++){
		attsToKeep[i + numAttsL] = i;
	}
}

void *joinWorker(void *args){

	JoinUtil* jU = (JoinUtil*)args;
	Record* lr = new Record();
	Record* rr = new Record();
	Record* jr = new Record();
	Pipe* outPipeL = new Pipe(100);
	OrderMaker *sortorder = new OrderMaker();

	DBFile dbfile;
	dbfile.Create("jointemp", heap, NULL);
	while(jU->inPipeR->Remove(rr)){
		dbfile.Add(*rr);
	}
	dbfile.Close();

	jU->inPipeL->Remove(lr);
	dbfile.GetNext(*rr);

	int numAttsL = numAttsL = ((int *) lr->bits)[1] / sizeof(int) -1;
	int numAttsR = ((int *) rr->bits)[1] / sizeof(int) -1;
	int attsToKeep[numAttsL + numAttsR];
	getJoinAttsToKeep(attsToKeep, numAttsL, numAttsR);

	ComparisonEngine ceng;

	do{
		dbfile.Open("jointemp");
		dbfile.MoveFirst();
		do{
			if (ceng.Compare(lr, rr, jU->literal, jU->selOp)) {	
				jr->MergeRecords(lr, rr, numAttsL, numAttsR, attsToKeep, numAttsL + numAttsR, numAttsL);
				jU->outPipe->Insert(jr);
			}
		}while(dbfile.GetNext(*rr));
		dbfile.Close();
	}while (jU->inPipeL->Remove(lr));
	jU->outPipe->ShutDown();
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	JoinUtil* jU = new JoinUtil(&inPipeL, &inPipeR, &outPipe, &selOp, &literal);
	pthread_create(&thread, NULL, joinWorker, (void *)jU);	
}

void Join::WaitUntilDone () {
	pthread_join (thread, NULL);
}
