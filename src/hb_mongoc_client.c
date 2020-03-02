//
//  hb_mongoc_client.c
//  hbmongoc
//
//  Created by Teo Fonrouge on 9/1/17.
//  Copyright © 2017 Teo Fonrouge. All rights reserved.
//

#include "hb_mongoc_client.h"
#include "hb_mongoc_uri.h"
#include "hb_mongoc.h"

HB_FUNC( MONGOC_CLIENT_COMMAND_SIMPLE )
{
    mongoc_client_t * client = mongoc_hbparam( 1, _hbmongoc_client_t_ );
    bson_t * command = bson_hbparam( 3, HB_IT_ANY );
    const char * db_name = hb_parc( 2 );

    if ( client && db_name && command && HB_ISBYREF( 5 ) ) {
        const mongoc_read_prefs_t * read_prefs = mongoc_hbparam( 4, _hbmongoc_read_prefs_t_ );
        bson_t reply;
        bson_error_t error;

        bool result = mongoc_client_command_simple( client, db_name, command, read_prefs, &reply, &error);

        hbmongoc_return_byref_bson( 5, bson_copy( &reply ) );
        bson_destroy(&reply);
        bson_hbstor_byref_error( 6, &error, result );

        hb_retl( result );

    } else {
        HBMONGOC_ERR_ARGS();
    }

    if ( command && ! HB_ISPOINTER( 3 ) ) {
        bson_destroy( command );
    }
}

HB_FUNC( MONGOC_CLIENT_DESTROY )
{
    PHB_MONGOC client = hbmongoc_param( 1, _hbmongoc_client_t_ );

    if( client ) {
        mongoc_client_destroy( client->p );
        client->p = NULL;
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_CLIENT_GET_COLLECTION )
{
    mongoc_client_t * client = mongoc_hbparam( 1, _hbmongoc_client_t_ );
    const char * db = hb_parc( 2 );
    const char * szCollection = hb_parc( 3 );

    if ( client && db && szCollection ) {
        mongoc_collection_t * collection = mongoc_client_get_collection( client, db, szCollection);
        if ( collection ) {
            PHB_MONGOC phCollection = hbmongoc_new_dataContainer( _hbmongoc_collection_t_, collection );
            hb_retptrGC( phCollection );
        } else {
            hb_ret();
        }
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_CLIENT_GET_DATABASE )
{
    mongoc_client_t * client = mongoc_hbparam( 1, _hbmongoc_client_t_ );
    const char * name = hb_parc( 2 );

    if ( client && name ) {
        mongoc_database_t * database = mongoc_client_get_database( client, name );
        if ( database ) {
            PHB_MONGOC phDatabase = hbmongoc_new_dataContainer( _hbmongoc_database_t_, database );
            hb_retptrGC( phDatabase );
        } else {
            hb_ret();
        }
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC(MONGOC_CLIENT_GET_URI) {
    mongoc_client_t * client = mongoc_hbparam(1, _hbmongoc_client_t_);
    
    if (client) {
        const mongoc_uri_t * uri = mongoc_client_get_uri(client);
        if ( uri ) {
            PHB_MONGOC phURI = hbmongoc_new_dataContainer(_hbmongoc_uri_t_, mongoc_uri_copy(uri));
            hb_retptrGC(phURI);
        } else {
            hb_ret();
        }
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_CLIENT_NEW )
{
    const char *uri_string = hb_parc( 1 );

    if ( uri_string ) {
        mongoc_client_t * client = mongoc_client_new( uri_string );
        if ( client ) {
            PHB_MONGOC phClient = hbmongoc_new_dataContainer( _hbmongoc_client_t_, client );
            hb_retptrGC( phClient );
        } else {
            hb_ret();
        }
    } else {
//        HBMONGOC_ERR_NOFUNC();
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_CLIENT_NEW_FROM_URI )
{
    const mongoc_uri_t * uri = mongoc_hbparam( 1, _hbmongoc_uri_t_ );

    if ( uri ) {
        mongoc_client_t * client = mongoc_client_new_from_uri( uri );
        if ( client ) {
            PHB_MONGOC phClient = hbmongoc_new_dataContainer( _hbmongoc_client_t_, client );
            hb_retptrGC( phClient );
        } else {
            hb_ret();
        }
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_CLIENT_SET_APPNAME )
#if MONGOC_CHECK_VERSION( 1, 5, 0 )
{
    mongoc_client_t * client = mongoc_hbparam( 1, _hbmongoc_client_t_ );
    const char * appname = hb_parc( 2 );

    if ( client && appname ) {
        bool result = mongoc_client_set_appname( client, appname );
        hb_retl( result );
    } else {
        HBMONGOC_ERR_ARGS();
    }
}
#else
{
    HBMONGOC_ERR_NOFUNC();
}
#endif
