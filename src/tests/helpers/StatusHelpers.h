// SPDX-License-Identifier: BSD-2-Clause
//
// This code has been produced by the European Spallation Source
// and its partner institutes under the BSD 2 Clause License.
//
// See LICENSE.md at the top level for license information.
//
// Screaming Udder!                              https://esss.se

#pragma once

namespace flatbuffers {
class DetachedBuffer;
}

namespace Status {
struct StatusInfo;
}

Status::StatusInfo
deserialiseStatusMessage(flatbuffers::DetachedBuffer const &Message);
