//
//  hb_mongo_cursor.c
//  hbmongoc
//
//  Created by Teo Fonrouge on 9/5/17.
//  Copyright © 2017 Teo Fonrouge. All rights reserved.
//

#include "hb_mongo_cursor.h"
#include "hb_mongoc.h"

HB_FUNC(MONGOC_CURSOR_DESTROY) {
    mongoc_cursor_t * cursor = mongoc_hbparam(1, _hbmongoc_cursor_t_);
    
    if (cursor) {
        mongoc_cursor_destroy(cursor);
    }
}

HB_FUNC( MONGOC_CURSOR_ERROR )
{
    mongoc_cursor_t * cursor = mongoc_hbparam( 1, _hbmongoc_cursor_t_ );

    if (cursor && HB_ISBYREF(2)) {
        bson_error_t error;
        bool result = mongoc_cursor_error(cursor, &error);
        bson_hbstor_byref_error( 2, &error, ! result );
        hb_retl(result);
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_CURSOR_NEXT )
{
    mongoc_cursor_t * cursor = mongoc_hbparam( 1, _hbmongoc_cursor_t_ );

    if ( cursor && HB_ISBYREF( 2 ) ) {

        const bson_t * doc;

        bool result = mongoc_cursor_next( cursor, &doc );

        if ( result ) {
            hbmongoc_return_byref_bson( 2, bson_copy( doc ) );
        } else {
            hb_stor( 2 );
        }

        hb_retl( result );
    } else {
        HBMONGOC_ERR_ARGS();
    }
}
