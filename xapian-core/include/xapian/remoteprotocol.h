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

#include <xapian/database.h>
#include <xapian/visibility.h>
#include <xapian/intrusive_ptr.h>
#include <xapian/registry.h>

#include <vector>
#include <string>


/// Class to throw when we receive the connection closing message.
struct ConnectionClosed { };


class XAPIAN_VISIBILITY_DEFAULT RemoteProtocol {
  public:
    class Internal;
    Xapian::Internal::intrusive_ptr<Internal> internal;

    /// Accept a message from the client.
    virtual int get_message(double timeout, std::string & result, int required_type) = 0;

    /// Send a message to the client, with specific end_time.
    virtual void send_message(int type, const std::string &message, double end_time) = 0;

    virtual Xapian::Database * get_db() = 0;
    virtual Xapian::WritableDatabase * get_wdb() = 0;
    virtual void release_db(Xapian::Database *db) = 0;
    virtual void select_db(const std::vector<std::string> &dbpaths, bool writable, int flags) = 0;

    virtual void shutdown() = 0;

    /// Get the registry used for (un)serialisation.
    const Xapian::Registry & get_registry() const;

    /// Set the registry used for (un)serialisation.
    void set_registry(const Xapian::Registry & reg_);

    bool is_writable();
    void set_dbpaths(const std::vector<std::string> &dbpaths);

    void send_greeting();
    void send_exception(const std::string & exception);
    void run_one();

    RemoteProtocol(const std::vector<std::string> &dbpaths,
		 double active_timeout,
		 double idle_timeout,
		 bool writable = false);

    virtual ~RemoteProtocol();

    static int get_major_version();
    static int get_minor_version();
};

#endif // XAPIAN_INCLUDED_REMOTEPROTOCOL_H
