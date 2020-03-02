//
//  hb_mongoc_uri.c
//  hbmongoc
//
//  Created by Teo Fonrouge on 9/1/17.
//  Copyright © 2017 Teo Fonrouge. All rights reserved.
//

#include "hb_mongoc_uri.h"
#include "hb_mongoc.h"

HB_FUNC( MONGOC_URI_NEW )
{
    const char * uri_string = hb_parc( 1 );

    if ( uri_string ) {
        mongoc_uri_t * uri = mongoc_uri_new( uri_string );
        if ( uri ) {
            PHB_MONGOC phURI = hbmongoc_new_dataContainer( _hbmongoc_uri_t_, uri );
            hb_retptrGC( phURI );
        } else {
            hb_ret();
        }
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC(MONGOC_URI_GET_STRING) {
    const mongoc_uri_t * uri = mongoc_hbparam( 1, _hbmongoc_uri_t_ );

    if (uri) {
        hb_retc(mongoc_uri_get_string(uri));
    } else {
        HBMONGOC_ERR_ARGS();
    }
}
