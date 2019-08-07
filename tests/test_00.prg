
#include "hbmongoc.ch"

PROCEDURE main()
    LOCAL client
    LOCAL collection
    LOCAL filter
    LOCAL opts
    LOCAL cursor
    LOCAL error
    LOCAL bson
    LOCAL _id

    /* REQUIRED to initialize mongoc internals */
    mongoc_init()

    client := mongoc_client_new( "mongodb://192.168.0.1" )

    collection := mongoc_client_get_collection( client, "hbmongoc", "test_00" )

    mongoc_collection_drop( collection )

        CLS

        bson := bson_new()
        HB_BSON_APPEND( bson, "datetime", hb_dateTime() )
        HB_BSON_APPEND( bson, "date", date() )
        ? hb_tton(hb_dateTime()), hb_tToUnix(hb_dateTime())
        BSON_APPEND_DATE_TIME( bson, "dtToUnix", hb_tToUnix(hb_dateTime()) )
        hb_bson_append( bson, "integer", 2^32 + 1 )
        BSON_APPEND_INT32( bson, "integer32", 2^32 + 1 )
        hb_bson_append( bson, "long", hb_tToUnix(hb_dateTime()) )
        BSON_APPEND_INT64( bson, "long64", hb_tToUnix(hb_dateTime()) )
        hb_bson_append( bson, "double", 1.0 )

        IF ! mongoc_collection_insert( collection, MONGOC_INSERT_NONE, bson, nil, @error )
            ? "error:", HB_BSON_ERROR_MESSAGE( error )
        ENDIF

        filter := bson_new()
        opts := bson_new()

        IF mongoc_check_version( 1, 5, 0 )
            cursor := mongoc_collection_find_with_opts( collection, filter, opts )
        ELSE
            cursor := mongoc_collection_find( collection, nil, nil, nil, nil, filter )
        ENDIF

        WHILE mongoc_cursor_next( cursor, @bson )
            _id := hb_bson_as_hash( bson, .T. )["_id"]["$oid"]
            ? _id
            ?
            ? "BSON_AS_JSON"
            ? bson_as_json( bson )
            IF bson_check_version( 1, 7, 0 )
                ?
                ? "BSON_AS_CANONICAL_EXTENDED_JSON"
                ? bson_as_canonical_extended_json( bson )
                ?
                ? "BSON_AS_RELAXED_EXTENDED_JSON"
                ? bson_as_relaxed_extended_json( bson )
            ENDIF
        ENDDO

    WAIT

    /* REQUIRED to cleanup mongoc internals */
    mongoc_cleanup()

RETURN
