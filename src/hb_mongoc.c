//
//  hb_mongoc.c
//  hbmongoc
//
//  Created by Teo Fonrouge on 8/26/17.
//  Copyright © 2017 Teo Fonrouge. All rights reserved.
//

#include "hb_mongoc.h"
#include "hbjson.h"

enum hb_return_bson_value_type { _HBRETVAL_BSON_, _HBRETVAL_JSON_, _HBRETVAL_HASH_ };

static bool s_mongoc_inited = false;
static enum hb_return_bson_value_type s_hbmongoc_return_bson_value_type = _HBRETVAL_BSON_;

static const char * _STR_BSON_ = "BSON";
static const char * _STR_JSON_ = "JSON";
static const char * _STR_HASH_ = "HASH";

static bool s_cancel_gc_cursor = false;

static HB_GARBAGE_FUNC( hbmongoc_funcs_destroy )
{
    PHB_MONGOC phMongoc = Cargo;

    if ( phMongoc && phMongoc->p ) {
        switch (phMongoc->type) {
            case _hbmongoc_client_t_:
                mongoc_client_destroy( ( mongoc_client_t * ) phMongoc->p );
                break;
            case _hbmongoc_database_t_:
                mongoc_database_destroy( ( mongoc_database_t * ) phMongoc->p );
                break;
            case _hbmongoc_collection_t_:
                mongoc_collection_destroy( ( mongoc_collection_t * ) phMongoc->p );
                break;
            case _hbmongoc_uri_t_:
                mongoc_uri_destroy( ( mongoc_uri_t * ) phMongoc->p );
                break;
            case _hbmongoc_cursor_t_:
                if (!s_cancel_gc_cursor) {
                    mongoc_cursor_destroy( ( mongoc_cursor_t * ) phMongoc->p );
                }
                break;
            case _hbmongoc_write_concern_t_:
                mongoc_write_concern_destroy( ( mongoc_write_concern_t * ) phMongoc->p );
                break;
            case _hbmongoc_read_prefs_t_:
                mongoc_read_prefs_destroy( ( mongoc_read_prefs_t * ) phMongoc->p );
                break;
            case _hbmongoc_bulk_operation_t_:
                mongoc_bulk_operation_destroy( ( mongoc_bulk_operation_t * ) phMongoc->p );
                break;
        }
        phMongoc->p = NULL;
    }
}

static const HB_GC_FUNCS s_gc_mongoc_funcs = {
    hbmongoc_funcs_destroy,
    hb_gcDummyMark
};

PHB_MONGOC hbmongoc_new_dataContainer( hbmongoc_t_ type, void * p )
{
    if ( p ) {
        PHB_MONGOC phMongo = hb_gcAllocate( sizeof( HB_MONGOC ), &s_gc_mongoc_funcs );

        phMongo->type = type;
        phMongo->p = p;

        return phMongo;
    } else {
        HBMONGOC_ERR_ARGS();
    }
    return NULL;
}

static void hbmongoc_check_inited()
{
    if ( ! s_mongoc_inited ) {
        mongoc_init();
        s_mongoc_inited = true;
    }
}

PHB_MONGOC hbmongoc_param( int iParam, hbmongoc_t_ type )
{
    PHB_MONGOC phMongo = hb_parptrGC( &s_gc_mongoc_funcs, iParam );

    return phMongo && phMongo->type == type ? phMongo : NULL;
}

PHB_BSON hbmongoc_return_byref_bson( int iParam, bson_t * bson )
{
    PHB_BSON phBson = NULL;
    char * szJSON = NULL;

    switch ( s_hbmongoc_return_bson_value_type ) {
        case _HBRETVAL_JSON_:
            szJSON = hbbson_as_json( bson );
            if ( szJSON ) {
                hb_storc( szJSON, iParam );
                bson_free( szJSON );
            } else {
                hb_stor( iParam );
            }
            bson_destroy( bson );
            break;
        case _HBRETVAL_BSON_:
            phBson = hbbson_new_dataContainer( _hbbson_t_, bson );
            hb_storptrGC( phBson, iParam );
            break;
        case _HBRETVAL_HASH_:
            szJSON = hbbson_as_json( bson );
            PHB_ITEM pItem = hb_itemNew( NULL );
            if ( szJSON ) {
                hb_jsonDecode( szJSON, pItem );
                bson_free( szJSON );
                hb_itemParamStoreRelease( iParam, pItem );
            } else {
                hb_stor( iParam );
            }
            bson_destroy( bson );
            break;
    }
    return phBson;
}

void * mongoc_hbparam( int iParam, hbmongoc_t_ type )
{
    PHB_MONGOC phMongoc = hbmongoc_param( iParam, type );

    if ( phMongoc && phMongoc->p ) {
        return phMongoc->p;
    }
    return NULL;
}


/* Harbour API */

HB_FUNC(HB_MONGOC_CANCEL_GC_CURSOR) {
    PHB_ITEM pItem = hb_param(1, HB_IT_LOGICAL);
    
    if (pItem) {
        s_cancel_gc_cursor = hb_parl(1);
    }
    
    if (pItem || hb_pcount() == 0) {
        hb_retl(s_cancel_gc_cursor);
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( HB_NUMTYPE )
{
    PHB_ITEM pItem = hb_param( 1, HB_IT_NUMERIC );

    if ( pItem ) {
        switch ( HB_ITEM_TYPE( pItem ) ) {
            case HB_IT_INTEGER:
                hb_retc( "I" );
                break;
            case HB_IT_LONG:
                hb_retc( "L" );
                break;
            case HB_IT_DOUBLE:
                hb_retc( "D" );
                break;
        }
    }
}

HB_FUNC( HB_MONGOC_SET_RETURN_BSON_VALUE_TYPE )
{
    if ( hb_pcount() > 0 ) {
        if ( hb_stricmp( hb_parc( 1 ), _STR_JSON_ ) == 0 ) {
            s_hbmongoc_return_bson_value_type = _HBRETVAL_JSON_;
        } else if ( hb_stricmp( hb_parc( 1 ), _STR_BSON_ ) == 0 ) {
            s_hbmongoc_return_bson_value_type = _HBRETVAL_BSON_;
        } else if ( hb_stricmp( hb_parc( 1 ), _STR_HASH_ ) == 0 ) {
            s_hbmongoc_return_bson_value_type = _HBRETVAL_HASH_;
        } else {
            HBMONGOC_ERR_ARGS();
        }
    }
    switch ( s_hbmongoc_return_bson_value_type ) {
        case _HBRETVAL_JSON_:
            hb_retc( _STR_JSON_ );
            break;
        case _HBRETVAL_BSON_:
            hb_retc( _STR_BSON_ );
            break;
        case _HBRETVAL_HASH_:
            hb_retc( _STR_HASH_ );
            break;
    }
}

HB_FUNC( MONGOC_CHECK_VERSION )
{
    if ( HB_ISNUM( 1 ) && HB_ISNUM( 2 ) && HB_ISNUM( 3 ) ) {
        hb_retl( mongoc_check_version( hb_parni( 1 ), hb_parni( 2 ), hb_parni( 3 ) ) );
    } else {
        HBMONGOC_ERR_ARGS();
    }
}

HB_FUNC( MONGOC_CLEANUP )
{
    if( s_mongoc_inited ) {
        mongoc_cleanup();
    }
}

HB_FUNC( MONGOC_GET_MAJOR_VERSION )
{
    hb_retni( mongoc_get_major_version() );
}

HB_FUNC( MONGOC_GET_MICRO_VERSION )
{
    hb_retni( mongoc_get_micro_version() );
}

HB_FUNC( MONGOC_GET_MINOR_VERSION )
{
    hb_retni( mongoc_get_minor_version() );
}

HB_FUNC( MONGOC_GET_VERSION )
{
    hb_retc( mongoc_get_version() );
}

HB_FUNC( MONGOC_INIT )
{
    hbmongoc_check_inited();
}
