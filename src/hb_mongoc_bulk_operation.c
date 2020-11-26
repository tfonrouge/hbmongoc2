//
//  hb_mongoc_bulk_operation.c
//  hbmongoc
//
//  Created by Teo Fonrouge on 9/7/17.
//  Copyright © 2017 Teo Fonrouge. All rights reserved.
//

#include "hb_mongoc_bulk_operation.h"
#include "hb_mongoc.h"

HB_FUNC( MONGOC_BULK_OPERATION_DESTROY )
{
    PHB_MONGOC phBulk = hbmongoc_param( 1, _hbmongoc_bulk_operation_t_ );

    if ( phBulk ) {
        mongoc_bulk_operation_destroy( ( mongoc_bulk_operation_t * ) phBulk->p );
        phBulk->p = NULL;
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_BULK_OPERATION_EXECUTE )
{
    mongoc_bulk_operation_t * bulk = mongoc_hbparam( 1, _hbmongoc_bulk_operation_t_ );

    if ( bulk && HB_ISBYREF( 2 ) ) {
        bson_t reply;
        bson_error_t error;

        uint32_t server_id = mongoc_bulk_operation_execute( bulk, &reply, &error );

        hbmongoc_return_byref_bson( 2, bson_copy(&reply) );
        bson_destroy(&reply);
        bson_hbstor_byref_error( 3, &error, server_id != 0 );

        hb_retni( server_id );

    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_BULK_OPERATION_INSERT )
{
    mongoc_bulk_operation_t * bulk = mongoc_hbparam( 1, _hbmongoc_bulk_operation_t_ );
    bson_t * document = bson_hbparam( 2, HB_IT_ANY );

    if ( bulk && document ) {
        mongoc_bulk_operation_insert( bulk, document );
    } else {
        HBMONGOC_ERR_ARGS();
    }

    if ( document && ! HB_ISPOINTER( 2 ) ) {
        bson_destroy( document );
    }
}

HB_FUNC( MONGOC_BULK_OPERATION_INSERT_WITH_OPTS )
{
    mongoc_bulk_operation_t * bulk = mongoc_hbparam( 1, _hbmongoc_bulk_operation_t_ );
    bson_t * document = bson_hbparam( 2, HB_IT_ANY );

    if ( bulk && document ) {
        bson_t * opts = bson_hbparam( 3, HB_IT_ANY );
        bson_error_t error;

        HB_BOOL result = mongoc_bulk_operation_insert_with_opts( bulk, document, opts, &error );

        bson_hbstor_byref_error( 4, &error, result );

        hb_retl( result );

        if ( opts && ! HB_ISPOINTER( 3 ) ) {
            bson_destroy( opts );
        }

    } else {
        HBMONGOC_ERR_ARGS();
    }

    if ( document && ! HB_ISPOINTER( 2 ) ) {
        bson_destroy( document );
    }
}

HB_FUNC(MONGOC_BULK_OPERATION_UPDATE_ONE_WITH_OPTS) {
    mongoc_bulk_operation_t * bulk = mongoc_hbparam( 1, _hbmongoc_bulk_operation_t_ );
    bson_t * selector = bson_hbparam( 2, HB_IT_ANY );
    bson_t * document = bson_hbparam( 3, HB_IT_ANY );

    if (bulk && selector && document) {
        bson_t * opts = bson_hbparam(4, HB_IT_ANY);
        bson_error_t error;
        
        HB_BOOL result = mongoc_bulk_operation_update_one_with_opts(bulk, selector, document, opts, &error);

        bson_hbstor_byref_error( 5, &error, result );

        hb_retl( result );

        if ( opts && ! HB_ISPOINTER( 4 ) ) {
            bson_destroy( opts );
        }

    } else {
        HBMONGOC_ERR_ARGS();
    }

    if (selector && ! HB_ISPOINTER(2)) {
        bson_destroy(selector);
    }
    if (document && ! HB_ISPOINTER(3)) {
        bson_destroy(document);
    }
}
