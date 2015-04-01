/** @file remoteserver.cc
 *  @brief Xapian remote backend server base class
 */
/* Copyright (C) 2006,2007,2008,2009,2010,2011,2012,2013,2014,2015 Olly Betts
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
#include "remoteserver.h"

#include "xapian/constants.h"
#include "xapian/database.h"
#include "xapian/error.h"

#include "safeerrno.h"
#include <signal.h>
#include <string>

#include "omassert.h"
#include "realtime.h"
#include "serialise.h"
#include "str.h"


Xapian::Database *
RemoteServer::get_db(bool writable_) {
    if (writable_) {
	return wdb;
    } else {
	return db;
    }
}

void
RemoteServer::select_db(const std::vector<std::string> &dbpaths_, bool writable_, int flags) {
    if (writable_) {
    	wdb = new Xapian::WritableDatabase(dbpaths_[0], flags);
    	delete db;
    	db = wdb;
    } else {
	wdb = NULL;
	Xapian::Database * db_ = new Xapian::Database(dbpaths_[0], flags);
	delete db;
	db = db_;
	// Build a better description than Database::get_description() gives
	// in the variable context.  FIXME: improve Database::get_description()
	// and then just use that instead.
	context = dbpaths_[0];
	if (!writable) {
	    std::vector<std::string>::const_iterator i(dbpaths_.begin());
	    for (++i; i != dbpaths_.end(); ++i) {
		db->add_database(Xapian::Database(*i));
		context += ' ';
		context += *i;
	    }
	} else {
	    AssertEq(dbpaths_.size(), 1); // Expecting exactly one database.
	}
    }
    dbpaths = dbpaths_;
}

RemoteServer::RemoteServer(const std::vector<std::string> &dbpaths_,
			   int fdin_, int fdout_,
			   double active_timeout_, double idle_timeout_,
			   bool writable_)
    : RemoteConnection(fdin_, fdout_, std::string()),
      RemoteProtocol(dbpaths_, active_timeout_, idle_timeout_, writable_),
      db(NULL), wdb(NULL)
{
    // Catch errors opening the database and propagate them to the client.
    try {
	Assert(!dbpaths_.empty());
	// We always open the database read-only to start with.  If we're
	// writable, the client can ask to be upgraded to write access once
	// connected if it wants it.
	select_db(dbpaths_, false, Xapian::DB_OPEN);
    } catch (const Xapian::Error &err) {
	// Propagate the exception to the client.
	send_message(REPLY_EXCEPTION, serialise_error(err));
	// And rethrow it so our caller can log it and close the connection.
	throw;
    }

#ifndef __WIN32__
    // It's simplest to just ignore SIGPIPE.  We'll still know if the
    // connection dies because we'll get EPIPE back from write().
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR)
	throw Xapian::NetworkError("Couldn't set SIGPIPE to SIG_IGN", errno);
#endif

    // Send greeting message.
    msg_update(std::string());
}

RemoteServer::~RemoteServer()
{
    if (db != NULL) delete db;
    // wdb is either NULL or equal to db, so we shouldn't delete it too!
    if (matchstate != NULL) {
	MatchState *matchstate_ = static_cast<MatchState *>(matchstate);
	// matchstate_->db is equal to db, so we shouldn't delete it too!
	delete matchstate_;
    }
}

message_type
RemoteServer::get_message(double timeout, string & result,
			  message_type required_type_)
{
    double end_time = RealTime::end_time(timeout);
    int type = RemoteConnection::get_message(result, end_time);

    // Handle "shutdown connection" message here.  Treat EOF here for a read-only
    // database the same way since a read-only client just closes the
    // connection when done.
    if (type == MSG_SHUTDOWN || (type < 0 && wdb == NULL))
	throw ConnectionClosed();
    if (type < 0)
	throw Xapian::NetworkError("Connection closed unexpectedly");
    if (type >= MSG_MAX) {
	std::string errmsg("Invalid message type ");
	errmsg += str(type);
	throw Xapian::NetworkError(errmsg);
    }
    if (required_type_ != MSG_MAX && type != int(required_type_)) {
	std::string errmsg("Expecting message type ");
	errmsg += str(int(required_type_));
	errmsg += ", got ";
	errmsg += str(int(type));
	throw Xapian::NetworkError(errmsg);
    }
    return static_cast<message_type>(type);
}

void
RemoteServer::send_message(reply_type type, const std::string &message)
{
    double end_time = RealTime::end_time(active_timeout);
    unsigned char type_as_char = static_cast<unsigned char>(type);
    RemoteConnection::send_message(type_as_char, message, end_time);
}

void
RemoteServer::send_message(reply_type type, const std::string &message, double end_time) {
    unsigned char type_as_char = static_cast<unsigned char>(type);
    RemoteConnection::send_message(type_as_char, message, end_time);
}


void
RemoteServer::run()
{
    while (true) {
    	run_one();
    }
}
