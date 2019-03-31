#include "test.h"
#include "BigQ.h"
#include "RelOp.h"
#include <pthread.h>
#include "BigQ.h"

Attribute IA = { "int", Int };
Attribute SA = { "string", String };
Attribute DA = { "double", Double };

int clear_pipe(Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove(&rec)) {
		if (print) {
			rec.Print(schema);
		}
		cnt++;
	}
	return cnt;
}

int clear_pipe(Pipe &in_pipe, Schema *schema, Function &func, bool print) {
	Record rec;
	int cnt = 0;
	double sum = 0;
	while (in_pipe.Remove(&rec)) {
		if (print) {
			rec.Print(schema);
		}
		int ival = 0; double dval = 0;
		func.Apply(rec, ival, dval);
		sum += (ival + dval);
		cnt++;
	}
	cout << " Sum: " << sum << endl;
	return cnt;
}
int pipesz = 100; // buffer sz allowed for each pipe
int buffsz = 100; // pages of memory allowed for operations

SelectFile SF_ps, SF_p, SF_s, SF_o, SF_li, SF_c;
DBFile dbf_ps, dbf_p, dbf_s, dbf_o, dbf_li, dbf_c;
Pipe _ps(pipesz), _p(pipesz), _s(pipesz), _o(pipesz), _li(pipesz), _c(pipesz);
CNF cnf_ps, cnf_p, cnf_s, cnf_o, cnf_li, cnf_c;
Record lit_ps, lit_p, lit_s, lit_o, lit_li, lit_c;
Function func_ps, func_p, func_s, func_o, func_li, func_c;

int pAtts = 9;
int psAtts = 5;
int liAtts = 16;
int oAtts = 9;
int sAtts = 7;
int cAtts = 8;
int nAtts = 4;
int rAtts = 3;

void init_SF_ps(char *pred_str, int numpgs) {
	dbf_ps.Open(ps->path());
	get_cnf(pred_str, ps->schema(), cnf_ps, lit_ps);
	SF_ps.Use_n_Pages(numpgs);
}

void init_SF_p(char *pred_str, int numpgs) {
	dbf_p.Open(p->path());
	get_cnf(pred_str, p->schema(), cnf_p, lit_p);
	SF_p.Use_n_Pages(numpgs);
}

void init_SF_s(char *pred_str, int numpgs) {
	dbf_s.Open(s->path());
	get_cnf(pred_str, s->schema(), cnf_s, lit_s);
	SF_s.Use_n_Pages(numpgs);
}

void init_SF_o(char *pred_str, int numpgs) {
	dbf_o.Open(o->path());
	get_cnf(pred_str, o->schema(), cnf_o, lit_o);
	SF_o.Use_n_Pages(numpgs);
}

void init_SF_li(char *pred_str, int numpgs) {
	dbf_li.Open(li->path());
	get_cnf(pred_str, li->schema(), cnf_li, lit_li);
	SF_li.Use_n_Pages(numpgs);
}

void init_SF_c(char *pred_str, int numpgs) {
	dbf_c.Open(c->path());
	get_cnf(pred_str, c->schema(), cnf_c, lit_c);
	SF_c.Use_n_Pages(numpgs);
}

TEST(DBFileGoogleTest1, Query1) {

	char *pred_ps = "(ps_supplycost < 10.03)";
	init_SF_ps(pred_ps, 100);

	/*cout << "cnf -- ";*/
	cnf_ps.Print();

	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps);
	SF_ps.WaitUntilDone();

	int cnt = clear_pipe(_ps, ps->schema(), true);
	cout << "\n\n query1 returned " << cnt << " records \n";

	dbf_ps.Close();
	EXPECT_EQ(cnt, 7986);

}

TEST(DBFileGoogleTest2, Query2) {

	cout << "q2 \n";

	char *pred_p = "(p_retailprice > 931.01) AND (p_retailprice < 931.3)";
	init_SF_p(pred_p, 100);

	Project P_p;
	Pipe _out(pipesz);
	int keepMe[] = { 0,1,7 };
	int numAttsIn = pAtts;
	int numAttsOut = 3;
	P_p.Use_n_Pages(buffsz);

	SF_p.Run(dbf_p, _p, cnf_p, lit_p);
	P_p.Run(_p, _out, keepMe, numAttsIn, numAttsOut);

	SF_p.WaitUntilDone();
	P_p.WaitUntilDone();

	Attribute att3[] = { IA, SA, DA };
	Schema out_sch("out_sch", numAttsOut, att3);
	// int cnt = clear_pipe (_out, p->schema (), true);
	int cnt = clear_pipe(_out, &out_sch, true);

	cout << "\n\n query2 returned " << cnt << " records \n";

	dbf_p.Close();
	EXPECT_EQ(22, cnt);

}


TEST(DBFileGoogleTest3, Query3) {

	char *pred_s = "(s_suppkey = s_suppkey)";
	init_SF_s(pred_s, 100);

	Sum T;
	// _s (input pipe)
	Pipe _out(1);
	Function func;
	char *str_sum = "(s_acctbal + (s_acctbal * 1.05))";
	get_cnf(str_sum, s->schema(), func);
	func.Print();
	T.Use_n_Pages(1);
	SF_s.Run(dbf_s, _s, cnf_s, lit_s);
	T.Run(_s, _out, func);

	SF_s.WaitUntilDone();
	T.WaitUntilDone();

	Schema out_sch("out_sch", 1, &DA);
	int cnt = clear_pipe(_out, &out_sch, true);

	cout << "\n\n query3 returned " << cnt << " records \n";

	dbf_s.Close();
	EXPECT_EQ(cnt, 1);
}

TEST(DBFileGoogleTest4, Query4) {

	cout << " query4 \n";

	char *pred_s = "(s_suppkey = s_suppkey)";
	Schema *testSchema = new Schema("catalog", "supplier");
	init_SF_s(pred_s, 100);

	char *pred_ps = "(ps_suppkey = ps_suppkey)";
	init_SF_ps(pred_ps, 100);

	SF_s.Run(dbf_s, _s, cnf_s, lit_s); // 10k recs qualified
	SF_ps.Run(dbf_ps, _ps, cnf_ps, lit_ps); // 161 recs qualified

	Join J;
	// left _s
	// right _ps
	Pipe _s_ps(pipesz);
	CNF cnf_p_ps;
	Record lit_p_ps;
	get_cnf("(s_suppkey = ps_suppkey)", s->schema(), ps->schema(), cnf_p_ps, lit_p_ps);

	int outAtts = sAtts + psAtts;
	Attribute ps_supplycost = { "ps_supplycost", Double };
	Attribute joinatt[] = { IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA };
	Schema join_sch("join_sch", outAtts, joinatt);

	Sum T;
	// _s (input pipe)
	Pipe _out(1);
	Function func;
	char *str_sum = "(ps_supplycost)";
	get_cnf(str_sum, &join_sch, func);
	func.Print();
	T.Use_n_Pages(1);

	J.Run(_s, _ps, _s_ps, cnf_p_ps, lit_p_ps);
	T.Run(_s_ps, _out, func);


	SF_ps.WaitUntilDone();
	J.WaitUntilDone();
	T.WaitUntilDone();

	Schema sum_sch("sum_sch", 1, &DA);
	int cnt = clear_pipe(_out, &sum_sch, true);
	cout << " query4 returned " << cnt << " recs \n";
	EXPECT_EQ(cnt, 1);
}


int main(int argc, char** argv) {

	::testing::InitGoogleTest(&argc, argv);
	setup();

	relation *rel_ptr[] = { n, r, c, p, ps, o, li };
	rel = rel_ptr[4];

	return RUN_ALL_TESTS();
}

