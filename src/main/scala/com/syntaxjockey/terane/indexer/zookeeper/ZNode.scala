/*
 * *
 *  * Copyright (c) 2010-${YEAR} Michael Frank <msfrank@syntaxjockey.com>
 *  *
 *  * This file is part of Terane.
 *  *
 *  * Terane is free software: you can redistribute it and/or modify
 *  * it under the terms of the GNU General Public License as published by
 *  * the Free Software Foundation, either version 3 of the License, or
 *  * (at your option) any later version.
 *  *
 *  * Terane is distributed in the hope that it will be useful,
 *  * but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  * GNU General Public License for more details.
 *  *
 *  * You should have received a copy of the GNU General Public License
 *  * along with Terane.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package com.syntaxjockey.terane.indexer.zookeeper

import org.apache.zookeeper.data.Stat

case class ZNode(czxid: Long,
                 mzxid: Long,
                 ctime: Long,
                 mtime: Long,
                 version: Int,
                 cversion: Int,
                 aversion: Int,
                 ephemeralOwner: Long,
                 dataLength: Int,
                 numChildren: Int,
                 pzxid: Long)

object ZNode {
  implicit def stat2ZNode(stat: Stat): ZNode = {
    ZNode(stat.getCzxid,
      stat.getMzxid,
      stat.getCtime,
      stat.getMtime,
      stat.getVersion,
      stat.getCversion,
      stat.getAversion,
      stat.getEphemeralOwner,
      stat.getDataLength,
      stat.getNumChildren,
      stat.getPzxid
    )
  }
}