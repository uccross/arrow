/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <arrow-flight-glib/common.h>

G_BEGIN_DECLS


#define GAFLIGHT_TYPE_SERVER_OPTIONS (gaflight_server_options_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServerOptions,
                         gaflight_server_options,
                         GAFLIGHT,
                         SERVER_OPTIONS,
                         GObject)
struct _GAFlightServerOptionsClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
GAFlightServerOptions *
gaflight_server_options_new(GAFlightLocation *location);


#define GAFLIGHT_TYPE_SERVER (gaflight_server_get_type())
G_DECLARE_DERIVABLE_TYPE(GAFlightServer,
                         gaflight_server,
                         GAFLIGHT,
                         SERVER,
                         GObject)
struct _GAFlightServerClass
{
  GObjectClass parent_class;
};

GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_server_listen(GAFlightServer *server,
                       GAFlightServerOptions *options,
                       GError **error);
GARROW_AVAILABLE_IN_5_0
gint
gaflight_server_get_port(GAFlightServer *server);
GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_server_shutdown(GAFlightServer *server,
                         GError **error);
GARROW_AVAILABLE_IN_5_0
gboolean
gaflight_server_wait(GAFlightServer *server,
                     GError **error);


G_END_DECLS
