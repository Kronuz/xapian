/** @file remoteprotocol.h
 *  @brief Remote protocol version and message numbers
 */
/* Copyright (C) 2006,2007,2008,2009,2010,2011,2013,2014,2015 Olly Betts
 * Copyright (C) 2007,2010 Lemur Consulting Ltd
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

#ifndef XAPIAN_INCLUDED_REMOTEPROTOCOL_H
#define XAPIAN_INCLUDED_REMOTEPROTOCOL_H

#include "xapian/database.h"
#include "xapian/visibility.h"
#include "xapian/registry.h"

#include <vector>
#include <string>


// Versions:
// 21: Overhauled remote backend supporting WritableDatabase
// 22: Lossless double serialisation
// 23: Support get_lastdocid() on remote databases
// 24: Support for OP_VALUE_RANGE in query serialisation
// 25: Support for delete_document and replace_document with unique term
// 26: Tweak delete_document with unique term; delta encode rset and termpos
// 27: Support for postlists (always passes the whole list across)
// 28: Pass document length in reply to MSG_TERMLIST
// 29: Serialisation of Xapian::Error includes error_string
// 30: Add minor protocol version numbers, to reduce need for client upgrades
// 30.1: Pass the prefix parameter for MSG_ALLTERMS, and use it.
// 30.2: New REPLY_DELETEDOCUMENT returns MSG_DONE to allow exceptions.
// 30.3: New MSG_GETMSET which passes check_at_least parameter.
// 30.4: New query operator OP_SCALE_WEIGHT.
// 30.5: New MSG_GETMSET which expects MSet's percent_factor to be returned.
// 30.6: Support for OP_VALUE_GE and OP_VALUE_LE in query serialisation
// 31: 1.1.0 Clean up for Xapian 1.1.0
// 32: 1.1.1 Serialise termfreq and reltermfreqs together in serialise_stats.
// 33: 1.1.3 Support for passing matchspies over the remote connection.
// 34: 1.1.4 Support for metadata over with remote databases.
// 35: 1.1.5 Support for add_spelling() and remove_spelling().
// 35.1: 1.2.4 Support for metadata_keys_begin().
// 36: 1.3.0 REPLY_UPDATE and REPLY_GREETING merged, and more...
// 37: 1.3.1 Prefix-compress termlists.
// 38: 1.3.2 Stats serialisation now includes collection freq, and more...
// 39: 1.3.3 New query operator OP_WILDCARD; sort keys in serialised MSet.
#define XAPIAN_REMOTE_PROTOCOL_MAJOR_VERSION 39
#define XAPIAN_REMOTE_PROTOCOL_MINOR_VERSION 0

/** Message types (client -> server).
 *
 *  When modifying this list, you probably need to update the array of function
 *  pointers in net/remoteserver.cc too.
 */
enum message_type {
    MSG_ALLTERMS,		// All Terms
    MSG_COLLFREQ,		// Get Collection Frequency
    MSG_DOCUMENT,		// Get Document
    MSG_TERMEXISTS,		// Term Exists?
    MSG_TERMFREQ,		// Get Term Frequency
    MSG_VALUESTATS,		// Get value statistics
    MSG_KEEPALIVE,		// Keep-alive
    MSG_DOCLENGTH,		// Get Doc Length
    MSG_QUERY,			// Run Query
    MSG_TERMLIST,		// Get TermList
    MSG_POSITIONLIST,		// Get PositionList
    MSG_POSTLIST,		// Get PostList
    MSG_REOPEN,			// Reopen
    MSG_UPDATE,			// Get Updated DocCount and AvLength
    MSG_ADDDOCUMENT,		// Add Document
    MSG_CANCEL,			// Cancel
    MSG_DELETEDOCUMENTTERM,	// Delete Document by term
    MSG_COMMIT,			// Commit
    MSG_REPLACEDOCUMENT,	// Replace Document
    MSG_REPLACEDOCUMENTTERM,	// Replace Document by term
    MSG_DELETEDOCUMENT,		// Delete Document
    MSG_WRITEACCESS,		// Upgrade to WritableDatabase
    MSG_GETMETADATA,		// Get metadata
    MSG_SETMETADATA,		// Set metadata
    MSG_ADDSPELLING,		// Add a spelling
    MSG_REMOVESPELLING,		// Remove a spelling
    MSG_GETMSET,		// Get MSet
    MSG_SHUTDOWN,		// Shutdown
    MSG_METADATAKEYLIST,	// Iterator for metadata keys
    MSG_FREQS,			// Get termfreq and collfreq
    MSG_UNIQUETERMS,		// Get number of unique terms in doc
    MSG_SELECT,			// Select current database
    MSG_MAX
};

/// Reply types (server -> client).
enum reply_type {
    REPLY_UPDATE,		// Updated database stats
    REPLY_EXCEPTION,		// Exception
    REPLY_DONE,			// Done sending list
    REPLY_ALLTERMS,		// All Terms
    REPLY_COLLFREQ,		// Get Collection Frequency
    REPLY_DOCDATA,		// Get Document
    REPLY_TERMDOESNTEXIST,	// Term Doesn't Exist
    REPLY_TERMEXISTS,		// Term Exists
    REPLY_TERMFREQ,		// Get Term Frequency
    REPLY_VALUESTATS,		// Value statistics
    REPLY_DOCLENGTH,		// Get Doc Length
    REPLY_STATS,		// Stats
    REPLY_TERMLIST,		// Get Termlist
    REPLY_POSITIONLIST,		// Get PositionList
    REPLY_POSTLISTSTART,	// Start of a postlist
    REPLY_POSTLISTITEM,		// Item in body of a postlist
    REPLY_VALUE,		// Document Value
    REPLY_ADDDOCUMENT,		// Add Document
    REPLY_RESULTS,		// Results (MSet)
    REPLY_METADATA,		// Metadata
    REPLY_METADATAKEYLIST,	// Iterator for metadata keys
    REPLY_FREQS,		// Get termfreq and collfreq
    REPLY_UNIQUETERMS,		// Get number of unique terms in doc
    REPLY_MAX
};


/// Class to throw when we receive the connection closing message.
struct ConnectionClosed { };


class XAPIAN_VISIBILITY_DEFAULT RemoteProtocol {
    void *matchstate;
    message_type required_type;

  protected:
    /// Accept a message from the client.
    virtual message_type get_message(double timeout, std::string & result,
				     message_type required_type = MSG_MAX) = 0;

    /// Send a message to the client.
    virtual void send_message(reply_type type, const std::string &message) = 0;

    /// Send a message to the client, with specific end_time.
    virtual void send_message(reply_type type, const std::string &message,
			      double end_time) = 0;

    virtual Xapian::Database * get_db(bool) = 0;
    virtual void release_db(Xapian::Database *) = 0;
    virtual void select_db(const std::vector<std::string> &, bool, int) = 0;
    virtual void shutdown() {};

    void cleanup();

    // all terms
    void msg_allterms(const std::string & message);

    // get document
    void msg_document(const std::string & message);

    // term exists?
    void msg_termexists(const std::string & message);

    // get collection freq
    void msg_collfreq(const std::string & message);

    // get termfreq
    void msg_termfreq(const std::string & message);

    // get termfreq and collection freq
    void msg_freqs(const std::string & message);

    // get value statistics
    void msg_valuestats(const std::string & message);

    // keep alive
    void msg_keepalive(const std::string & message);

    // get doclength
    void msg_doclength(const std::string & message);

    // set the query; return the mset
    void msg_query(const std::string & message);

    // get termlist
    void msg_termlist(const std::string & message);

    // get postlist
    void msg_postlist(const std::string & message);

    // get positionlist
    void msg_positionlist(const std::string &message);

    // get write access
    void msg_writeaccess(const std::string & message);

    // reopen
    void msg_reopen(const std::string & message);

    // get updated doccount and avlength
    void msg_update(const std::string &message);

    // commit
    void msg_commit(const std::string & message);

    // cancel
    void msg_cancel(const std::string &message);

    // add document
    void msg_adddocument(const std::string & message);

    // delete document
    void msg_deletedocument(const std::string & message);

    // delete document with unique term
    void msg_deletedocumentterm(const std::string & message);

    // replace document
    void msg_replacedocument(const std::string & message);

    // replace document with unique term
    void msg_replacedocumentterm(const std::string & message);

    // get metadata
    void msg_getmetadata(const std::string & message);

    // read metadata key list
    void msg_openmetadatakeylist(const std::string & message);

    // set metadata
    void msg_setmetadata(const std::string & message);

    // add a spelling
    void msg_addspelling(const std::string & message);

    // remove a spelling
    void msg_removespelling(const std::string & message);

    // get number of unique terms
    void msg_uniqueterms(const std::string & message);

    // select the active database
    void msg_select(const std::string & message);

    void msg_getmset(const std::string & message);

    void msg_shutdown(const std::string & message);

    void run_one();

  public:
    /// The registry, which allows unserialisation of user subclasses.
    Xapian::Registry reg;

    std::vector<std::string> dbpaths;

    /// Do we support writing?
    bool writable;

    /** Timeout for actions during a conversation.
     *
     *  The timeout is specified in seconds.  If the timeout is exceeded then a
     *  Xapian::NetworkTimeoutError is thrown.
     */
    double active_timeout;

    /** Timeout while waiting for a new action from the client.
     *
     *  The timeout is specified in seconds.  If the timeout is exceeded then a
     *  Xapian::NetworkTimeoutError is thrown.
     */
    double idle_timeout;

    RemoteProtocol(const std::vector<std::string> &dbpaths,
		 double active_timeout_,
		 double idle_timeout_,
		 bool writable = false);

    virtual ~RemoteProtocol();
};

#endif // XAPIAN_INCLUDED_REMOTEPROTOCOL_H
