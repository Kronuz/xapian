#include "progclient.h"
#include <om/omenquire.h>
#include <typeinfo>

ostream &
operator<<(ostream &os, const OmMSetItem &mitem)
{
    os << mitem.wt << " " << mitem.did;
    return os;
}

ostream &
operator<<(ostream &os, const OmMSet &mset)
{
    copy(mset.items.begin(), mset.items.end(),
	 ostream_iterator<OmMSetItem>(os, "\n"));
    return os;
}

int main()
{
    try {
	OmDatabase db;
	vector<string> params;
	params.push_back("prog");
	params.push_back("./omnetclient");
	params.push_back("text1.txt");
	db.add_database("net", params);

	params.pop_back();
	params.push_back("text2.txt");
	db.add_database("net", params);

	OmEnquire enq(db);

	enq.set_query(OmQuery("word"));

	OmMSet mset(enq.get_mset(0, 10));

	cout << mset;
    } catch (OmError &e) {
	cout << "OmError exception (" << typeid(e).name()
	     << "): " << e.get_msg() << endl;
    }
}
