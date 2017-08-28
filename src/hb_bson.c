//
//  hb_bson.c
//  hbmongoc
//
//  Created by Teo Fonrouge on 8/26/17.
//  Copyright © 2017 Teo Fonrouge. All rights reserved.
//

#include "hb_bson.h"

#include "hbapi.h"
#include "hbapiitm.h"
#include "hbapierr.h"
#include "hbapifs.h"
#include "hbapistr.h"
#include "hbstack.h"
#include "hbvm.h"

#include <bson.h>

#define BSON_ERR_ARGS()  ( hb_errRT_BASE_SubstR( EG_ARG, 3012, NULL, HB_ERR_FUNCNAME, HB_ERR_ARGS_BASEPARAMS ) )

/*
 hbbson_destroy
 */
static HB_GARBAGE_FUNC( hbbson_gc_func )
{
    PHB_BSON pBson = Cargo;

    if ( pBson && pBson->p ) {
        switch (pBson->type_t) {
            case _hb_bson_t_:
                bson_destroy( ( bson_t * ) pBson->p );
                break;
        }
        pBson->p = NULL;
    }
}

/*
 s_gc_mongoc_client_t
 */
static const HB_GC_FUNCS s_gc_bson_funcs = {
    hbbson_gc_func,
    hb_gcDummyMark
};

PHB_BSON hbbson_param( int iParam, hbbson_t_ type )
{
    PHB_BSON pBson = NULL;

    if ( HB_ISPOINTER( iParam ) ) {
        pBson = hb_parptrGC( &s_gc_bson_funcs, iParam );
    } else {
        if( HB_ISCHAR( iParam ) ) {
            const char * szJSON = hb_parc( iParam );
            bson_t * bson = bson_new_from_json( ( const uint8_t * ) szJSON, -1, NULL );
            pBson = hbbson_new_dataContainer( _hb_bson_t_, bson );
        }
    }

    return pBson && pBson->type_t == type && pBson->p ? pBson : NULL;
}

PHB_BSON hbbson_new_dataContainer( hbbson_t_ type, void * p )
{
    PHB_BSON pMongoc = hb_gcAllocate( sizeof( HB_BSON ), &s_gc_bson_funcs );

    pMongoc->type_t = type;
    pMongoc->p = p;

    return pMongoc;
}

HB_FUNC( BSON_AS_JSON )
{
    PHB_BSON pBson = hbbson_param( 1, _hb_bson_t_ );

    if ( pBson ) {
        char * szJSON = bson_as_json( ( bson_t * ) pBson->p, NULL );
        if( szJSON ) {
            hb_retc( szJSON );
            bson_free( szJSON );
        }
    } else {
        BSON_ERR_ARGS();
    }
}

#if BSON_CHECK_VERSION( 1, 7, 0 )
HB_FUNC( BSON_AS_CANONICAL_EXTENDED_JSON )
{
    PHB_BSON pBson = hbbson_param( 1, _hb_bson_t_ );

    if ( pBson ) {
        char * szJSON = bson_as_canonical_extended_json( ( bson_t * ) pBson->p, NULL );
        if ( szJSON ) {
            hb_retc( szJSON );
            bson_free( szJSON );
        }
    } else {
        BSON_ERR_ARGS();
    }
}
#endif
