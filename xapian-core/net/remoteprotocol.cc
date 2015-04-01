/** @file remoteprotocol.cc
 *  @brief Xapian remote backend server base class
 */
/* Copyright (C) 2006,2007,2008,2009,2010,2011,2012,2013,2014 Olly Betts
 * Copyright (C) 2006,2007,2009,2010 Lemur Consulting Ltd
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 */

#include <config.h>
#include "xapian/remoteprotocol.h"

#include "xapian/constants.h"
#include "xapian/database.h"
#include "xapian/enquire.h"
#include "xapian/error.h"
#include "xapian/matchspy.h"
#include "xapian/query.h"
#include "xapian/valueiterator.h"

#include <vector>

#include "autoptr.h"
#include "length.h"
#include "matcher/multimatch.h"
#include "noreturn.h"
#include "serialise.h"
#include "serialise-double.h"
#include "str.h"
#include "stringutils.h"
#include "weight/weightinternal.h"


XAPIAN_NORETURN(static void throw_read_only());
static void
throw_read_only()
{
    throw Xapian::InvalidOperationError("Server is read-only");
}

XAPIAN_NORETURN(static void throw_no_db());
static void
throw_no_db()
{
    throw Xapian::InvalidOperationError("Server has no open database");
}

/** Structure holding a match and a list of match spies.
 *
 *  The main reason for the existence of this structure is to allow passing
 *  match state between query and mset.
 */
struct MatchState {
    Xapian::Database *db;
    MultiMatch *match;
    vector<Xapian::Internal::opt_intrusive_ptr<Xapian::MatchSpy>> spies;
    Xapian::Weight *wt;
    MatchState() : match(NULL), wt(NULL) {}
    ~MatchState() {
	if (match) {
		delete match;
	}
	if (wt) {
		delete wt;
	}
    }
};


typedef void (RemoteProtocol::* dispatch_func)(const string &);

RemoteProtocol::RemoteProtocol(const std::vector<std::string> &dbpaths_,
			       double active_timeout_,
			       double idle_timeout_,
			       bool writable_)
    : matchstate(NULL), required_type(MSG_MAX),
      dbpaths(dbpaths_), writable(writable_),
      active_timeout(active_timeout_), idle_timeout(idle_timeout_)
{}

RemoteProtocol::~RemoteProtocol()
{
	cleanup();
}

void
RemoteProtocol::cleanup()
{
    if (matchstate != NULL) {
	MatchState *matchstate_ = static_cast<MatchState *>(matchstate);
	release_db(matchstate_->db);
	delete matchstate_;
    }
}

void
RemoteProtocol::run_one()
{
	try {
	    /* This list needs to be kept in the same order as the list of
	     * message types in "remoteprotocol.h". Note that messages at the
	     * end of the list in "remoteprotocol.h" can be omitted if they
	     * don't correspond to dispatch actions.
	     */
	    static const dispatch_func dispatch[] = {
		&RemoteProtocol::msg_allterms,
		&RemoteProtocol::msg_collfreq,
		&RemoteProtocol::msg_document,
		&RemoteProtocol::msg_termexists,
		&RemoteProtocol::msg_termfreq,
		&RemoteProtocol::msg_valuestats,
		&RemoteProtocol::msg_keepalive,
		&RemoteProtocol::msg_doclength,
		&RemoteProtocol::msg_query,
		&RemoteProtocol::msg_termlist,
		&RemoteProtocol::msg_positionlist,
		&RemoteProtocol::msg_postlist,
		&RemoteProtocol::msg_reopen,
		&RemoteProtocol::msg_update,
		&RemoteProtocol::msg_adddocument,
		&RemoteProtocol::msg_cancel,
		&RemoteProtocol::msg_deletedocumentterm,
		&RemoteProtocol::msg_commit,
		&RemoteProtocol::msg_replacedocument,
		&RemoteProtocol::msg_replacedocumentterm,
		&RemoteProtocol::msg_deletedocument,
		&RemoteProtocol::msg_writeaccess,
		&RemoteProtocol::msg_getmetadata,
		&RemoteProtocol::msg_setmetadata,
		&RemoteProtocol::msg_addspelling,
		&RemoteProtocol::msg_removespelling,
		&RemoteProtocol::msg_getmset,
		&RemoteProtocol::msg_shutdown,
		&RemoteProtocol::msg_openmetadatakeylist,
		&RemoteProtocol::msg_freqs,
		&RemoteProtocol::msg_uniqueterms,
		&RemoteProtocol::msg_select,
	    };

	    string message;
	    size_t type = get_message(idle_timeout, message, required_type);
	    if (type >= sizeof(dispatch)/sizeof(dispatch[0]) || !dispatch[type]) {
		string errmsg("Unexpected message type ");
		errmsg += str(type);
		throw Xapian::InvalidArgumentError(errmsg);
	    }
	    (this->*(dispatch[type]))(message);
	} catch (const Xapian::NetworkTimeoutError & e) {
	    try {
		// We've had a timeout, so the client may not be listening, so
		// set the end_time to 1 and if we can't send the message right
		// away, just exit and the client will cope.
		send_message(REPLY_EXCEPTION, serialise_error(e), 1.0);
	    } catch (...) {
	    }
	    // And rethrow it so our caller can log it and close the
	    // connection.
	    throw;
	} catch (const Xapian::NetworkError &) {
	    // All other network errors mean we are fatally confused and are
	    // unlikely to be able to communicate further across this
	    // connection.  So we don't try to propagate the error to the
	    // client, but instead just rethrow the exception so our caller can
	    // log it and close the connection.
	    throw;
	} catch (const Xapian::Error &e) {
	    // Propagate the exception to the client, then return to the main
	    // message handling loop.
	    send_message(REPLY_EXCEPTION, serialise_error(e));
	} catch (ConnectionClosed &) {
	    return;
	} catch (...) {
	    // Propagate an unknown exception to the client.
	    send_message(REPLY_EXCEPTION, string());
	    // And rethrow it so our caller can log it and close the
	    // connection.
	    throw;
	}
}


void
RemoteProtocol::msg_allterms(const string &message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    string prev = message;
    string reply;

    const string & prefix = message;
    const Xapian::TermIterator end = db_->allterms_end(prefix);
    for (Xapian::TermIterator t = db_->allterms_begin(prefix); t != end; ++t) {
	if (rare(prev.size() > 255))
	    prev.resize(255);
	const string & v = *t;
	size_t reuse = common_prefix_length(prev, v);
	reply = encode_length(t.get_termfreq());
	reply.append(1, char(reuse));
	reply.append(v, reuse, string::npos);
	send_message(REPLY_ALLTERMS, reply);
	prev = v;
    }

    send_message(REPLY_DONE, string());

    release_db(db_);
}

void
RemoteProtocol::msg_termlist(const string &message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::docid did;
    decode_length(&p, p_end, did);

    send_message(REPLY_DOCLENGTH, encode_length(db_->get_doclength(did)));
    string prev;
    const Xapian::TermIterator end = db_->termlist_end(did);
    for (Xapian::TermIterator t = db_->termlist_begin(did); t != end; ++t) {
	if (rare(prev.size() > 255))
	    prev.resize(255);
	const string & v = *t;
	size_t reuse = common_prefix_length(prev, v);
	string reply = encode_length(t.get_wdf());
	reply += encode_length(t.get_termfreq());
	reply.append(1, char(reuse));
	reply.append(v, reuse, string::npos);
	send_message(REPLY_TERMLIST, reply);
	prev = v;
    }

    send_message(REPLY_DONE, string());

    release_db(db_);
}

void
RemoteProtocol::msg_positionlist(const string &message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::docid did;
    decode_length(&p, p_end, did);
    string term(p, p_end - p);

    Xapian::termpos lastpos = static_cast<Xapian::termpos>(-1);
    const Xapian::PositionIterator end = db_->positionlist_end(did, term);
    for (Xapian::PositionIterator i = db_->positionlist_begin(did, term);
	 i != end; ++i) {
	Xapian::termpos pos = *i;
	send_message(REPLY_POSITIONLIST, encode_length(pos - lastpos - 1));
	lastpos = pos;
    }

    send_message(REPLY_DONE, string());

    release_db(db_);
}


void
RemoteProtocol::msg_select(const string &message)
{
    size_t len;
    const char *p = message.c_str();
    const char *p_end = p + message.size();

    std::vector<string> dbpaths_;

    while (p != p_end) {
	decode_length_and_check(&p, p_end, len);
	string dbpath(p, len);
	dbpaths_.push_back(dbpath);
	p += len;
    }

    select_db(dbpaths_, false, Xapian::DB_OPEN);

    msg_update(message);
}


void
RemoteProtocol::msg_postlist(const string &message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    const string & term = message;

    Xapian::doccount termfreq = db_->get_termfreq(term);
    Xapian::termcount collfreq = db_->get_collection_freq(term);
    send_message(REPLY_POSTLISTSTART, encode_length(termfreq) + encode_length(collfreq));

    Xapian::docid lastdocid = 0;
    const Xapian::PostingIterator end = db_->postlist_end(term);
    for (Xapian::PostingIterator i = db_->postlist_begin(term);
	 i != end; ++i) {

	Xapian::docid newdocid = *i;
	string reply = encode_length(newdocid - lastdocid - 1);
	reply += encode_length(i.get_wdf());

	send_message(REPLY_POSTLISTITEM, reply);
	lastdocid = newdocid;
    }

    send_message(REPLY_DONE, string());

    release_db(db_);
}

void
RemoteProtocol::msg_writeaccess(const string & msg)
{
    if (!writable)
	throw_read_only();

    int flags = Xapian::DB_OPEN;
    const char *p = msg.c_str();
    const char *p_end = p + msg.size();
    if (p != p_end) {
	unsigned flag_bits;
	decode_length(&p, p_end, flag_bits);
	flags |= flag_bits &~ Xapian::DB_ACTION_MASK_;
	if (p != p_end) {
	    throw Xapian::NetworkError("Junk at end of MSG_WRITEACCESS");
	}
    }

    select_db(dbpaths, true, flags);

    msg_update(msg);
}


void
RemoteProtocol::msg_reopen(const string & msg)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    if (!db_->reopen()) {
	send_message(REPLY_DONE, string());
	release_db(db_);
	return;
    }

    msg_update(msg);
    release_db(db_);
}

void
RemoteProtocol::msg_update(const string &)
{
    Xapian::Database * db_ = get_db(false);

    static const char protocol[2] = {
	char(XAPIAN_REMOTE_PROTOCOL_MAJOR_VERSION),
	char(XAPIAN_REMOTE_PROTOCOL_MINOR_VERSION)
    };

    string message(protocol, 2);

    if (db_) {
	Xapian::doccount num_docs = db_->get_doccount();
	message += encode_length(num_docs);
	message += encode_length(db_->get_lastdocid() - num_docs);
	Xapian::termcount doclen_lb = db_->get_doclength_lower_bound();
	message += encode_length(doclen_lb);
	message += encode_length(db_->get_doclength_upper_bound() - doclen_lb);
	message += (db_->has_positions() ? '1' : '0');
	// FIXME: clumsy to reverse calculate total_len like this:
	totlen_t total_len = totlen_t(db_->get_avlength() * db_->get_doccount() + .5);
	message += encode_length(total_len);
	//message += encode_length(db_->get_total_length());
	string uuid = db_->get_uuid();
	message += uuid;
    }

    send_message(REPLY_UPDATE, message);

    release_db(db_);
}

void
RemoteProtocol::msg_query(const string &message_in)
{
    const char *p = message_in.c_str();
    const char *p_end = p + message_in.size();

    // Unserialise the Query.
    size_t len;
    decode_length_and_check(&p, p_end, len);
    Xapian::Query query(Xapian::Query::unserialise(string(p, len), reg));
    p += len;

    // Unserialise assorted Enquire settings.
    Xapian::termcount qlen;
    decode_length(&p, p_end, qlen);

    Xapian::valueno collapse_max;
    decode_length(&p, p_end, collapse_max);

    Xapian::valueno collapse_key = Xapian::BAD_VALUENO;
    if (collapse_max)
	decode_length(&p, p_end, collapse_key);

    if (p_end - p < 4 || *p < '0' || *p > '2') {
	throw Xapian::NetworkError("bad message (docid_order)");
    }
    Xapian::Enquire::docid_order order;
    order = static_cast<Xapian::Enquire::docid_order>(*p++ - '0');

    Xapian::valueno sort_key;
    decode_length(&p, p_end, sort_key);

    if (*p < '0' || *p > '3') {
	throw Xapian::NetworkError("bad message (sort_by)");
    }
    Xapian::Enquire::Internal::sort_setting sort_by;
    sort_by = static_cast<Xapian::Enquire::Internal::sort_setting>(*p++ - '0');

    if (*p < '0' || *p > '1') {
	throw Xapian::NetworkError("bad message (sort_value_forward)");
    }
    bool sort_value_forward(*p++ != '0');

    double time_limit = unserialise_double(&p, p_end);

    int percent_cutoff = *p++;
    if (percent_cutoff < 0 || percent_cutoff > 100) {
	throw Xapian::NetworkError("bad message (percent_cutoff)");
    }

    double weight_cutoff = unserialise_double(&p, p_end);
    if (weight_cutoff < 0) {
	throw Xapian::NetworkError("bad message (weight_cutoff)");
    }

    // Unserialise the Weight object.
    decode_length_and_check(&p, p_end, len);
    string wtname(p, len);
    p += len;

    const Xapian::Weight * wttype = reg.get_weighting_scheme(wtname);
    if (wttype == NULL) {
	// Note: user weighting schemes should be registered by adding them to
	// a Registry, and setting the context using
	// RemoteServer::set_registry().
	throw Xapian::InvalidArgumentError("Weighting scheme " +
					   wtname + " not registered");
    }

    MatchState * matchstate_;
    if (matchstate != NULL) {
	matchstate_ = static_cast<MatchState *>(matchstate);
	release_db(matchstate_->db);
	delete matchstate_;
    }

    matchstate_ = new MatchState();
    matchstate = matchstate_;

    decode_length_and_check(&p, p_end, len);
    matchstate_->wt = wttype->unserialise(string(p, len));
    p += len;

    // Unserialise the RSet object.
    decode_length_and_check(&p, p_end, len);
    Xapian::RSet rset = unserialise_rset(string(p, len));
    p += len;

    // Unserialise any MatchSpy objects.
    while (p != p_end) {
	decode_length_and_check(&p, p_end, len);
	string spytype(p, len);
	const Xapian::MatchSpy * spyclass = reg.get_match_spy(spytype);
	if (spyclass == NULL) {
	    throw Xapian::InvalidArgumentError("Match spy " + spytype +
					       " not registered");
	}
	p += len;

	decode_length_and_check(&p, p_end, len);
	matchstate_->spies.push_back(spyclass->unserialise(string(p, len), reg));
	p += len;
    }

    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    Xapian::Weight::Internal local_stats;
    matchstate_->match = new MultiMatch(*db_, query, qlen, &rset, collapse_max, collapse_key,
		     percent_cutoff, weight_cutoff, order,
		     sort_key, sort_by, sort_value_forward, time_limit, NULL,
		     local_stats, matchstate_->wt, matchstate_->spies, false, false);
    matchstate_->db = db_;

    send_message(REPLY_STATS, serialise_stats(local_stats));

    required_type = MSG_GETMSET;
}

void
RemoteProtocol::msg_getmset(const string & msg)
{
    if (matchstate == NULL) {
	required_type = MSG_MAX;
	throw Xapian::NetworkError("Unexpected MSG_GETMSET");
    }

    MatchState *matchstate_ = static_cast<MatchState *>(matchstate);

    const char *p = msg.c_str();
    const char *p_end = p + msg.size();

    Xapian::termcount first;
    decode_length(&p, p_end, first);
    Xapian::termcount maxitems;
    decode_length(&p, p_end, maxitems);

    Xapian::termcount check_at_least;
    decode_length(&p, p_end, check_at_least);

    std::string message(p, p_end);
    AutoPtr<Xapian::Weight::Internal> total_stats(new Xapian::Weight::Internal);
    unserialise_stats(message, *(total_stats.get()));
    total_stats->set_bounds_from_db(*matchstate_->db);

    Xapian::MSet mset;
    matchstate_->match->get_mset(first, maxitems, check_at_least, mset, *(total_stats.get()), 0, 0);
    mset.internal->stats = total_stats.release();

    message.resize(0);
    for (auto i : matchstate_->spies) {
    	string spy_results = i->serialise_results();
	message += encode_length(spy_results.size());
	message += spy_results;
    }
    message += serialise_mset(mset);
    send_message(REPLY_RESULTS, message);

    matchstate = NULL;
    required_type = MSG_MAX;

    release_db(matchstate_->db);
    delete matchstate_;
}

void
RemoteProtocol::msg_document(const string &message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::docid did;
    decode_length(&p, p_end, did);

    Xapian::Document doc = db_->get_document(did);

    send_message(REPLY_DOCDATA, doc.get_data());

    Xapian::ValueIterator i;
    for (i = doc.values_begin(); i != doc.values_end(); ++i) {
	string item = encode_length(i.get_valueno());
	item += *i;
	send_message(REPLY_VALUE, item);
    }
    send_message(REPLY_DONE, string());

    release_db(db_);
}

void
RemoteProtocol::msg_keepalive(const string &)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    // Ensure *our* database stays alive, as it may contain remote databases!
    db_->keep_alive();
    send_message(REPLY_DONE, string());

    release_db(db_);
}

void
RemoteProtocol::msg_termexists(const string &term)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    send_message((db_->term_exists(term) ? REPLY_TERMEXISTS : REPLY_TERMDOESNTEXIST), string());

    release_db(db_);
}

void
RemoteProtocol::msg_collfreq(const string &term)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    send_message(REPLY_COLLFREQ, encode_length(db_->get_collection_freq(term)));

    release_db(db_);
}

void
RemoteProtocol::msg_termfreq(const string &term)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    send_message(REPLY_TERMFREQ, encode_length(db_->get_termfreq(term)));

    release_db(db_);
}

void
RemoteProtocol::msg_freqs(const string &term)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    string msg = encode_length(db_->get_termfreq(term));
    msg += encode_length(db_->get_collection_freq(term));
    send_message(REPLY_FREQS, msg);

    release_db(db_);
}

void
RemoteProtocol::msg_valuestats(const string & message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    const char *p = message.data();
    const char *p_end = p + message.size();
    while (p != p_end) {
	Xapian::valueno slot;
	decode_length(&p, p_end, slot);
	string message_out;
	message_out += encode_length(db_->get_value_freq(slot));
	string bound = db_->get_value_lower_bound(slot);
	message_out += encode_length(bound.size());
	message_out += bound;
	bound = db_->get_value_upper_bound(slot);
	message_out += encode_length(bound.size());
	message_out += bound;

	send_message(REPLY_VALUESTATS, message_out);
    }

    release_db(db_);
}

void
RemoteProtocol::msg_doclength(const string &message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::docid did;
    decode_length(&p, p_end, did);
    send_message(REPLY_DOCLENGTH, encode_length(db_->get_doclength(did)));

    release_db(db_);
}

void
RemoteProtocol::msg_uniqueterms(const string &message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::docid did;
    decode_length(&p, p_end, did);
    send_message(REPLY_UNIQUETERMS, encode_length(db_->get_unique_terms(did)));

    release_db(db_);
}

void
RemoteProtocol::msg_commit(const string &)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();

    wdb_->commit();

    send_message(REPLY_DONE, string());

    release_db(wdb_);
}

void
RemoteProtocol::msg_cancel(const string &)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();

    // We can't call cancel since that's an internal method, but this
    // has the same effect with minimal additional overhead.
    wdb_->begin_transaction(false);
    wdb_->cancel_transaction();

    release_db(wdb_);
}

void
RemoteProtocol::msg_adddocument(const string & message)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();

    Xapian::docid did = wdb_->add_document(unserialise_document(message));

    send_message(REPLY_ADDDOCUMENT, encode_length(did));

    release_db(wdb_);
}

void
RemoteProtocol::msg_deletedocument(const string & message)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();

    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::docid did;
    decode_length(&p, p_end, did);

    wdb_->delete_document(did);

    send_message(REPLY_DONE, string());

    release_db(wdb_);
}

void
RemoteProtocol::msg_deletedocumentterm(const string & message)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();

    wdb_->delete_document(message);

    release_db(wdb_);
}

void
RemoteProtocol::msg_replacedocument(const string & message)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();

    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::docid did;
    decode_length(&p, p_end, did);

    wdb_->replace_document(did, unserialise_document(string(p, p_end)));

    release_db(wdb_);
}

void
RemoteProtocol::msg_replacedocumentterm(const string & message)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();

    const char *p = message.data();
    const char *p_end = p + message.size();
    size_t len;
    decode_length_and_check(&p, p_end, len);
    string unique_term(p, len);
    p += len;

    Xapian::docid did = wdb_->replace_document(unique_term, unserialise_document(string(p, p_end)));

    send_message(REPLY_ADDDOCUMENT, encode_length(did));

    release_db(wdb_);
}

void
RemoteProtocol::msg_getmetadata(const string & message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    send_message(REPLY_METADATA, db_->get_metadata(message));

    release_db(db_);
}

void
RemoteProtocol::msg_openmetadatakeylist(const string & message)
{
    Xapian::Database * db_ = get_db(false);
    if (!db_)
	throw_no_db();

    string prev = message;
    string reply;

    const string & prefix = message;
    const Xapian::TermIterator end = db_->metadata_keys_end(prefix);
    Xapian::TermIterator t = db_->metadata_keys_begin(prefix);
    for (; t != end; ++t) {
	if (rare(prev.size() > 255))
	    prev.resize(255);
	const string & v = *t;
	size_t reuse = common_prefix_length(prev, v);
	reply.assign(1, char(reuse));
	reply.append(v, reuse, string::npos);
	send_message(REPLY_METADATAKEYLIST, reply);
	prev = v;
    }
    send_message(REPLY_DONE, string());

    release_db(db_);
}

void
RemoteProtocol::msg_setmetadata(const string & message)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();
    const char *p = message.data();
    const char *p_end = p + message.size();
    size_t keylen;
    decode_length_and_check(&p, p_end, keylen);
    string key(p, keylen);
    p += keylen;
    string val(p, p_end - p);
    wdb_->set_metadata(key, val);
    release_db(wdb_);
}

void
RemoteProtocol::msg_addspelling(const string & message)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();
    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::termcount freqinc;
    decode_length(&p, p_end, freqinc);
    wdb_->add_spelling(string(p, p_end - p), freqinc);
    release_db(wdb_);
}

void
RemoteProtocol::msg_removespelling(const string & message)
{
    Xapian::WritableDatabase * wdb_ = static_cast<Xapian::WritableDatabase *>(get_db(true));
    if (!wdb_)
	throw_read_only();
    const char *p = message.data();
    const char *p_end = p + message.size();
    Xapian::termcount freqdec;
    decode_length(&p, p_end, freqdec);
    wdb_->remove_spelling(string(p, p_end - p), freqdec);
    release_db(wdb_);
}

void
RemoteProtocol::msg_shutdown(const string &)
{
	shutdown();
}
