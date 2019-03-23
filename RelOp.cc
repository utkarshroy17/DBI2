#include "RelOp.h"
#include <pthread.h>
#include <iostream>

void *AddToOut(void *args) {
	
	sf_utils *sf = (sf_utils *)args;
	ComparisonEngine ceng;
	Record temp;
	Schema *testSchema = new Schema("catalog", "partsupp");

	cout << "inside thread exec" << endl;
	sf->inFile.MoveFirst();

	cout << "moved to first" << endl;
	
	sf->selOperator.Print();

	while (sf->inFile.GetNext(temp)) {

		if (ceng.Compare(&temp, &sf->literal, &sf->selOperator)) {	
			sf->outPipe->Insert(&temp);
		}
	}

	sf->outPipe->ShutDown();
}


void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	
	selOp.Print();

	//sf_utils args = { inFile, &outPipe, selOp, literal };
	sf_utils *args = new sf_utils;

	args->inFile = inFile;
	args->outPipe = &outPipe;
	args->literal = literal;
	args->selOperator = selOp;

	pthread_create(&thread, NULL, AddToOut, (void *) args);
}

void SelectFile::WaitUntilDone () {

	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {
	runLength = runlen;
}
