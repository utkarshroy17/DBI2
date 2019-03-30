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

void GetSumRec(Record *finalRec, Type resType, int intSum, double dblSum) {

	string rec;

	if (resType == Int) {

		rec = to_string(intSum);
		rec.append("|");

		Attribute IA = { "int", Int };
		Schema out_schema("out_sch", 1, &IA);
		finalRec->ComposeRecord(&out_schema, rec.c_str());
	}
	else if (resType == Double) {
		rec = to_string(dblSum);
		rec.append("|");

		Attribute DA = { "double", Double };
		Schema out_schema("out_sch", 1, &DA);
		finalRec->ComposeRecord(&out_schema, rec.c_str());
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

	cout << "shutting down select file" << endl;

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
	Record *finalRec = new Record();

	while (s->inPipe->Remove(temp)) {

		resType = s->computeMe.Apply(*temp, intParam, dblParam);

		if (resType == Int)
			intSum += intParam;
		else if (resType == Double)
			dblSum += dblParam;
	}

	GetSumRec(finalRec, resType, intSum, dblSum);

	cout << "double sum = " << dblSum << endl;
	cout << "int sum = " << intSum << endl;

	s->outPipe->Insert(finalRec);
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

	int intParam = 0, intSum = 0;
	double dblParam = 0, dblSum = 0;
	Type resType;	

	OrderMaker *sortorder = new OrderMaker();
	int whichAtts[] = { 3 };
	Type whichTypes[] = { Int };
	sortorder->Set(1, whichAtts, whichTypes);

	cout << "Inside group by thread" << endl;

	Pipe sortedPipe(100);
	BigQ bq(*gb->inPipe, sortedPipe, *sortorder, gb->runLength);

	Attribute attr;
	attr.name = (char *)"sum";
	attr.myType = resType;
	Schema *schema = new Schema((char *)"dummy", 1, &attr);

	int numAttsToKeep = gb->groupAtts->numAtts + 1;
	int *attsToKeep = new int[numAttsToKeep];
	attsToKeep[0] = 0;  //for sumRec
	cout << "[ 0";
	for (int i = 1; i < numAttsToKeep; i++)
	{
		attsToKeep[i] = gb->groupAtts->whichAtts[i - 1];
		cout << ", " << attsToKeep[i];
	}
	cout << "]" << endl;

	Record *finalRec = new Record();
	int count = 0;	

	if (sortedPipe.Remove(prev)) {
		
		cout << "sorted file first rec" << endl;
		resType = gb->computeMe->Apply(*prev, intParam, dblParam);

		if (resType == Int)
			intSum += intParam;
		else if (resType == Double)
			dblSum += dblParam;
		count++;
	}

	while (sortedPipe.Remove(temp)) {

		/*cout << "sorted file other recs" << endl;*/
		if (ceng.Compare(temp, prev, sortorder) != 0) {

			cout << "inside comparison" << endl;

			
			GetSumRec(finalRec, resType, intSum, dblSum);

			Record *tuple = new Record;
			tuple->MergeRecords(finalRec, prev, 1, gb->groupAtts->numAtts, attsToKeep, numAttsToKeep, 1);

			//tuple->Print(&join_sch);

			gb->outPipe->Insert(tuple);
			
			//cout << dblSum << endl;
			/*cout << "prev - ";
			prev->Print(gb->grpSchema);
			cout << "temp - ";
			temp->Print(gb->grpSchema);*/

			intSum = 0;
			dblSum = 0;
			count++;			
		}
		
		resType = gb->computeMe->Apply(*temp, intParam, dblParam);

		if (resType == Int)
			intSum += intParam;
		else if (resType == Double)
			dblSum += dblParam;
		
		//temp->Print(gb->grpSchema);
		prev->Copy(temp);
	}

	cout << "final count - " << count << endl;
	cout << "double sum - " << dblSum << endl;

	GetSumRec(finalRec, resType, intSum, dblSum);
		
	Record *tuple = new Record;
	tuple->MergeRecords(finalRec, prev, 1, gb->groupAtts->numAtts, attsToKeep, numAttsToKeep, 1);

	gb->outPipe->Insert(tuple);

	cout << "shutting group by" << endl;
	gb->outPipe->ShutDown();

}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe, Schema &grpSchema) {

	cout << "Inside group by run" << endl;
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	this->grpSchema = &grpSchema;

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
	int count = 0;

	while(jU->inPipeR->Remove(rr)){
		dbfile.Add(*rr);
		count++;
	}

	dbfile.Close();

	jU->inPipeL->Remove(lr);
	dbfile.GetNext(*rr);

	int numAttsL = ((int *) lr->bits)[1] / sizeof(int) -1;
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

	cout << "shutting join" << endl;
	jU->outPipe->ShutDown();
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	JoinUtil* jU = new JoinUtil(&inPipeL, &inPipeR, &outPipe, &selOp, &literal);
	pthread_create(&thread, NULL, joinWorker, (void *)jU);	
}

void Join::WaitUntilDone () {
	pthread_join (thread, NULL);
}
