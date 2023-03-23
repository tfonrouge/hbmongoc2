/*
* MongoDb RDD
*
* recNo must be NOT EQUAL to NIL otherwise en EOF state is set
*/

#include "dbinfo.ch"
#include "dbstruct.ch"
#include "error.ch"
#include "fileio.ch"
#include "hbtrace.ch"
#include "hbusrrdd.ch"
#include "rddsys.ch"

#define LEFTEQUAL( l, r )    iif( ValType( l ) $ "CM", Left( l, Len( r ) ) == r, l == r )

ANNOUNCE MONGODBRDD

#define COLLDATA_NAME           "name"
#define COLLDATA_COLLECTIONPTR  "collectionPtr"
#define COLLDATA_IDISOID        "idIsOid"
#define COLLDATA_FREESLOT       "freeSlot"
#define COLLDATA_RECINFO        "recInfo"
#define COLLDATA_OPENNUMBER     "openNumber"
#define COLLDATA_LOCKED         "locked"
#define COLLDATA_STRUCT         "struct"
#define COLLDATA_INDEXARRAY     "indexArray"

#define WADATA_COLLDATA      "collData"
#define WADATA_WORKAREA      "workArea"
#define WADATA_OPENINFO      "openInfo"
#define WADATA_RECNO         "recNo"
#define WADATA_BSONDOC       "bsonDoc"
#define WADATA_BOF           "bof"
#define WADATA_FORCEBOF      "forceBof"
#define WADATA_EOF           "eof"
#define WADATA_TOP           "top"
#define WADATA_BOTTOM        "bottom"
#define WADATA_FOUND         "found"
#define WADATA_LOCKS         "locks"
#define WADATA_INDEX         "index"
#define WADATA_WAORDINFO     "waOrdInfo"
#define WADATA_ORDRECNO      "ordRecNo"
#define WADATA_FILTERINFO    "filterInfo"
#define WADATA_LOCATE        "locate"

#define RECDATA_DELETED      1
#define RECDATA_LOCKED       2
#define RECDATA_SIZEOF       2

#define INDEX_TAG            1
#define INDEX_ORCR           2
#define INDEX_RECORDS        3
#define INDEX_SIZEOF         3

#define INDEXKEY_KEY         1
#define INDEXKEY_RECORD      2
#define INDEXKEY_SIZEOF      2

// #define WAOI_SCOPE_0         1
// #define WAOI_SCOPE_1         2
#define WAOI_SIZEOF          2

STATIC s_nRddID := -1

STATIC browseStateMap := {=>}
STATIC indexStateMap := {=>}

STATIC FUNCTION MDB_INIT( nRDD )
    USRRDD_RDDDATA(nRDD, { => })
RETURN HB_SUCCESS

STATIC FUNCTION MDB_COLLINIT()
    RETURN {;
        COLLDATA_NAME => NIL, ;  /* COLLDATA_NAME        */
        COLLDATA_COLLECTIONPTR => NIL, ;  /* COLLDATA_COLLECTIONPTR    */
        COLLDATA_IDISOID => NIL, ;  /* COLLDATA_IDISOID     */
        COLLDATA_FREESLOT => {}, ;   /* COLLDATA_FREESLOT    */
        COLLDATA_RECINFO => {}, ;   /* COLLDATA_RECINFO     */
        COLLDATA_OPENNUMBER => 0, ;    /* COLLDATA_OPENNUMBER  */
        COLLDATA_LOCKED => .F., ;  /* COLLDATA_LOCKED      */
        COLLDATA_STRUCT => {}, ;   /* COLLDATA_STRUCT - aStruct */
        COLLDATA_INDEXARRAY => {} ;    /* COLLDATA_INDEXARRAY       */
        }

STATIC FUNCTION MDB_WADATAINIT()
    RETURN { ;
        WADATA_COLLDATA => NIL, ; /* WADATA_COLLDATA */
        WADATA_WORKAREA => 0, ;   /* WADATA_WORKAREA */
        WADATA_OPENINFO => NIL, ; /* WADATA_OPENINFO */
        WADATA_RECNO => NIL, ; /* WADATA_RECNO */
        WADATA_BSONDOC => NIL, ; /* WADATA_BSONDOC */
        WADATA_BOF => .F., ; /* WADATA_BOF */
        WADATA_FORCEBOF => .F., ; /* WADATA_FORCEBOF - to solve an hack in dbf1.c */
        WADATA_EOF => .F., ; /* WADATA_EOF */
        WADATA_TOP => .F., ; /* WADATA_TOP */
        WADATA_BOTTOM => .F., ; /* WADATA_BOTTOM */
        WADATA_FOUND => .F., ; /* WADATA_FOUND */
        WADATA_LOCKS => {}, ;  /* WADATA_LOCKS */
        WADATA_INDEX => 0, ;   /* WADATA_INDEX */
        WADATA_WAORDINFO => {}, ;  /* WADATA_WAORDINFO */
        WADATA_ORDRECNO => 0, ;   /* WADATA_ORDRECNO */
        WADATA_FILTERINFO => NIL, ; /* WADATA_FILTERINFO */
        WADATA_LOCATE => Array( UR_SI_SIZE ) ; /* WADATA_LOCATE */
        }

STATIC FUNCTION MDB_WAOIINIT()
    RETURN { ;
        "scope0" => NIL, ; /* WAOI_SCOPE_0 */
        "scope1" => NIL,;  /* WAOI_SCOPE_1 */
        "seek" => {"key" => NIL, "soft" => NIL, "last" => NIL, "skipped" => NIL};
        }
     
STATIC FUNCTION MDB_NEW( pWA )

    /*
         * Set in our private AREA item the array with slot number and
         * BOF/EOF flags. There is no BOF support in HB_F* function so
         * we have to emulate it and there is no phantom record so we
         * cannot return EOF flag directly.
         */
     
    USRRDD_AREADATA( pWA, MDB_WADATAINIT() )
     
RETURN HB_SUCCESS

STATIC FUNCTION EmptyValue( cType, nLen, nDec )

    LOCAL xVal
 
    hb_default( @nLen, 0 )
    hb_default( @nDec, 0 )
 
    DO CASE
        CASE cType == "C" .OR. cType == "M"
            xVal := Space( nLen )
        CASE cType == "D"
            xVal := hb_SToD()
        CASE cType == "L"
            xVal := .F.
        CASE cType == "N"
            xVal := Val( Str( 0, nLen, nDec ) )
    ENDCASE
 
RETURN xVal
 
STATIC FUNCTION MDB_BOF( nWA, lBof )

    LOCAL aWAData := USRRDD_AREADATA( nWA )
 
    HB_TRACE( HB_TR_DEBUG, hb_StrFormat( "nWA: %1$d, lBof: %2$s", nWA, hb_ValToExp( lBof ) ) )
 
    /* This is a hack to protect from dbf1.c skipraw hack */
    IF aWAData[ WADATA_FORCEBOF ] .AND. lBof
        aWAData[ WADATA_BOF ] := lBof
        aWAData[ WADATA_FORCEBOF ] := .F.
    ELSE
        lBof := aWAData[ WADATA_BOF ]
    ENDIF
 
RETURN HB_SUCCESS

STATIC FUNCTION MDB_DELETED()
RETURN .F.
 
STATIC FUNCTION MDB_EOF( nWA, lEof )

    LOCAL aWAData := USRRDD_AREADATA( nWA )
 
    HB_TRACE( HB_TR_DEBUG, hb_StrFormat( "nWA: %1$d, lEof: %2$s", nWA, hb_ValToExp( lEof ) ) )
 
    lEof := aWAData[ WADATA_EOF ]
 
RETURN HB_SUCCESS

STATIC FUNCTION MDB_FOUND( nWa, lFound )
 
    lFound := USRRDD_AREADATA( nWA )[ WADATA_FOUND ]
 
RETURN HB_SUCCESS
  
STATIC FUNCTION MDB_GETVALUE( nWA, nField, xValue )

    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL aCollData := aWAData[ WADATA_COLLDATA ]
    LOCAL aStruct  := aCollData[ COLLDATA_STRUCT ]
 
    IF nField > 0 .AND. nField <= Len( aStruct )
        IF aWAData[ WADATA_EOF ] .OR. aWAData[WADATA_BSONDOC] == NIL
            /* We are at EOF position, return empty value */
            xValue := EmptyValue( aStruct[ nField ][ DBS_TYPE ], aStruct[ nField ][ DBS_LEN ], aStruct[ nField ][ DBS_DEC ] )
        ELSE
            xValue := aWAData[WADATA_BSONDOC][aStruct[nField][1]]
        ENDIF
        RETURN HB_SUCCESS
    ENDIF
 
RETURN HB_FAILURE

STATIC PROCEDURE setDbState(aWAData, hDoc)
    LOCAL _id
    LOCAL itm
    LOCAL s

    IF hDoc == NIL
        aWAData[WADATA_BSONDOC] := NIL
        aWAData[WADATA_RECNO]   := NIL
        aWAData[WADATA_EOF] := aWAData[WADATA_BOF] := .T.
        aWAData[WADATA_FOUND] := .F.
    ELSE
        IF aWAData[WADATA_COLLDATA][COLLDATA_IDISOID]
            _id := hDoc["_id"]["$oid"]
        ELSE
            _id := hDoc["_id"]
        ENDIF
        FOR EACH itm IN aWAData[WADATA_COLLDATA][COLLDATA_STRUCT]
            IF itm[2] == "C"
                s := hDoc[itm[1]]
                IF len(s) != itm[3]
                    hDoc[itm[1]] := padR(s, itm[3], " ")
                ENDIF
            ENDIF
        NEXT
        aWAData[WADATA_BSONDOC] := hDoc
        aWAData[WADATA_RECNO]   := _id
        aWAData[WADATA_EOF] := aWAData[WADATA_BOF] := .F.
    ENDIF
RETURN

STATIC FUNCTION MDB_GOTO( nWA, _id )
    LOCAL aWAData := USRRDD_AREADATA( nWA )
    LOCAL aCollData := aWAData[WADATA_COLLDATA]
    LOCAL cursor
    LOCAL ref
    LOCAL hDoc
    LOCAL itm

    FOR EACH itm IN aWAData[WADATA_WAORDINFO]
        itm["seek"]["skipped"] := NIL
    NEXT

    IF !empty(_id) 
        IF valType(_id) = "H" .AND. !hb_hHasKey(_id, "$oid")
            hDoc := _id
        ENDIF
        IF hDoc == NIL
            IF aWAData[WADATA_COLLDATA][COLLDATA_IDISOID] .AND. valType(_id) != "H"
                _id := {"$oid" => _id}
            ENDIF
            cursor := mongoc_collection_aggregate(aCollData[COLLDATA_COLLECTIONPTR], NIL, ;
                {;
                {"$match" => {"_id" => _id}},;
                {"$limit" => 1};
                };
                )
            WHILE mongoc_cursor_next(cursor, @ref)
                hDoc := hb_bson_as_hash(ref)
            ENDDO
        ENDIF
    ENDIF

    setDbState(aWAData, hDoc)

RETURN HB_SUCCESS

STATIC FUNCTION MDB_GOTOID( nWA, nRecord )
RETURN MDB_GOTO( nWA, nRecord )
 
STATIC FUNCTION MDB_GOTOP( nWA )
    LOCAL aWAData   := USRRDD_AREADATA( nWA )
    LOCAL aCollData := aWAData[ WADATA_COLLDATA ]
    LOCAL aIndexes  := aCollData[ COLLDATA_INDEXARRAY ]
    LOCAL nIndex    := aWAData[ WADATA_INDEX ]
    LOCAL nResult   := HB_SUCCESS
    LOCAL pipeline := {}
    LOCAL cursor
    LOCAL ref
    LOCAL hDoc
    LOCAL pCollection := aCollData[COLLDATA_COLLECTIONPTR]

    aWAData[ WADATA_BOF ]   := .F.
    aWAData[ WADATA_EOF ]   := .F.

    IF nIndex == 0
        AAdd(pipeline, {"$match" => {=>}})
        AAdd(pipeline, {"$sort" => {"_id" => 1}})
        AAdd(pipeline, {"$limit" => 1})
        cursor := mongoc_collection_aggregate(pCollection, NIL, pipeline)    
        WHILE mongoc_cursor_next(cursor, @ref)
            hDoc := hb_bson_as_hash(ref)
        ENDDO
        MDB_GOTO(nWA, hDoc)
    ELSEIF aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope0" ] == NIL
        IF Empty( aIndexes[ nIndex ][ INDEX_RECORDS ] )
            aWAData[ WADATA_ORDRECNO ] := 0
            nResult := MDB_GOTO( nWA, 0 )
        ELSE
            aWAData[ WADATA_ORDRECNO ] := 1
            nResult := MDB_GOTO( nWA, aIndexes[ nIndex ][ INDEX_RECORDS ][ 1 ][ INDEXKEY_RECORD ] )
        ENDIF
    ELSE
        aWAData[ WADATA_ORDRECNO ] := SeekScope( nWA, aIndexes[ nIndex ], aWAData[ WADATA_WAORDINFO ][ nIndex ], .F. )
        nResult := MDB_GOTO( nWA, aIndexes[ nIndex ][ INDEX_RECORDS ][ aWAData[ WADATA_ORDRECNO ] ][ INDEXKEY_RECORD ] )
    ENDIF
 
RETURN nResult

STATIC FUNCTION MDB_GOBOTTOM( nWA )
    LOCAL aWAData := USRRDD_AREADATA( nWA )
    LOCAL nIndex  := aWAData[ WADATA_INDEX ]
    LOCAL aCollData := aWAData[ WADATA_COLLDATA ]
    LOCAL aIndexes  := aCollData[ COLLDATA_INDEXARRAY ]
    LOCAL nResult
    LOCAL pipeline
    LOCAL cursor
    LOCAL ref
    LOCAL hDoc
    LOCAL pCollection := aCollData[COLLDATA_COLLECTIONPTR]

    aWAData[WADATA_BOF] := .F.
    aWAData[WADATA_EOF] := .F.

    IF nIndex == 0
        pipeline := {}
        AAdd(pipeline, {"$match" => {=>}})
        AAdd(pipeline, {"$sort" => {"_id" => -1}})
        AAdd(pipeline, {"$limit" => 1})
        cursor := mongoc_collection_aggregate(pCollection, NIL, pipeline)    
        WHILE mongoc_cursor_next(cursor, @ref)
            hDoc := hb_bson_as_hash(ref)
        ENDDO
        RETURN MDB_GOTO(nWA, hDoc)
    ENDIF

    IF aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope0" ] == NIL
        IF Empty( aIndexes[ nIndex ][ INDEX_RECORDS ] )
            aWAData[ WADATA_ORDRECNO ] := 0
            nResult := MDB_GOTO( nWA, 0 )
        ELSE
            aWAData[ WADATA_ORDRECNO ] := 1
            nResult := MDB_GOTO( nWA, aIndexes[ nIndex ][ INDEX_RECORDS ][ 1 ][ INDEXKEY_RECORD ] )
        ENDIF
    ELSE
        nResult := MDB_GOTO( nWA, SeekScope( nWA, aIndexes[ nIndex ], aWAData[ WADATA_WAORDINFO ][ nIndex ], .T. ) )
    ENDIF

RETURN nResult

#define FIELD_TYPES "CLDNFIBT@=^+YZQMVPWG"

STATIC FUNCTION MDB_OPEN( nWA, aOpenInfo )
    LOCAL hRDDData
    LOCAL aWAData := USRRDD_AREADATA(nWA)
    LOCAL aCollData
    LOCAL mdbStruct, aStruct, oError, aFieldStruct, aField, nResult
    LOCAL collName
    LOCAL symbName
    LOCAL itm

    collName := aOpenInfo[UR_OI_NAME]
    hRDDData := USRRDD_RDDDATA( USRRDD_ID( nWA ) )

    /* When there is no ALIAS we will create new one using file name */
    IF aOpenInfo[ UR_OI_ALIAS ] == NIL
        aOpenInfo[ UR_OI_ALIAS ] := collName
    ENDIF 

    IF !hb_hHasKey(hRDDData, collName)
        symbName := buildMdbStructName(collName)
        IF !__dynsIsFun(symbName)
            oError := ErrorNew()
            oError:GenCode     := EG_OPEN
            oError:SubCode     := 1000
            oError:Description := hb_langErrMsg( EG_OPEN ) + ", "+symbName+"() function not defined"
            oError:FileName    := aOpenInfo[ UR_OI_NAME ]
            oError:CanDefault  := .T.
            NetErr( .T. )
            UR_SUPER_ERROR( nWA, oError )
            RETURN HB_FAILURE
        ENDIF
        mdbStruct := __dynsN2Sym(symbName):exec(symbName)
        aStruct := mdbStruct[2]
        hRDDData[collName] := MDB_COLLINIT()
        hRDDData[collName][COLLDATA_NAME] := collName
        hRDDData[collName][COLLDATA_IDISOID] := mdbStruct[1]["_idType"] == "oid"
        hRDDData[collName][COLLDATA_STRUCT] := aStruct
        hRDDData[collName][COLLDATA_COLLECTIONPTR] := mongoc_database_get_collection(ArelEnv():database:getMongoDatabase(), collName)
        symbName := "mdbIndex" + collName
        IF __dynsIsFun(symbName)
            hRDDData[collName][COLLDATA_INDEXARRAY] := {}
            FOR EACH itm IN __dynsN2Sym(symbName):exec()
                mapIndexInfo(itm, aStruct)
                AAdd(hRDDData[collName][COLLDATA_INDEXARRAY], itm)
                AAdd(aWAData[WADATA_WAORDINFO], MDB_WAOIINIT())
            NEXT
        ENDIF
        buildMdbIndexes(collName)
    ELSE
        hRDDData[collName, COLLDATA_OPENNUMBER] += 1
        aStruct := aCollData[ COLLDATA_STRUCT ]
    ENDIF

    aCollData := hRDDData[ collName ]
 
    /* Set WorkArea Infos */
    aWAData[ WADATA_COLLDATA ] := aCollData   /* Put a reference to database */
    aWAData[ WADATA_WORKAREA ] := nWA
    aWAData[ WADATA_OPENINFO ] := aOpenInfo  /* Put open informations */
 
    /* Set fields */
    UR_SUPER_SETFIELDEXTENT( nWA, Len( aStruct ) )
 
    FOR EACH aFieldStruct IN aStruct
        aField := Array( UR_FI_SIZE )
        aField[ UR_FI_NAME ]    := aFieldStruct[ DBS_NAME ]
        // aField[ UR_FI_TYPE ]    := hb_Decode( aFieldStruct[ DBS_TYPE ], "C", HB_FT_STRING, "L", HB_FT_LOGICAL, "M", HB_FT_MEMO, "D", HB_FT_DATE, "B", HB_FT_DOUBLE, "N", iif( aFieldStruct[ DBS_DEC ] > 0, HB_FT_DOUBLE, HB_FT_INTEGER ) )
        aField[ UR_FI_TYPE ]    := Int(hb_at(aFieldStruct[DBS_TYPE], FIELD_TYPES))
        aField[ UR_FI_TYPEEXT ] := 0
        aField[ UR_FI_LEN ]     := aFieldStruct[ DBS_LEN ]
        aField[ UR_FI_DEC ]     := aFieldStruct[ DBS_DEC ]
        UR_SUPER_ADDFIELD( nWA, aField )
    NEXT
 
    /* Call SUPER OPEN to finish allocating work area (f.e.: alias settings) */
    nResult := UR_SUPER_OPEN( nWA, aOpenInfo )
 
    /* Add a new open number */
    aCollData[ COLLDATA_OPENNUMBER ]++
 
    /* File already opened in exclusive mode */
    /* I have to do this check here because, in case of error, MDB_CLOSE() is called however */
    IF aCollData[ COLLDATA_LOCKED ]
        oError := ErrorNew()
        oError:GenCode     := EG_OPEN
        oError:SubCode     := 1000
        oError:Description := hb_langErrMsg( EG_OPEN ) + "(" + ;
            hb_langErrMsg( EG_LOCK ) + " - already opened in exclusive mode)"
        oError:FileName    := aOpenInfo[ UR_OI_NAME ]
        oError:CanDefault  := .T.
        NetErr( .T. )
        UR_SUPER_ERROR( nWA, oError )
        RETURN HB_FAILURE
    ENDIF
 
    /* Open file in exclusive mode */
    IF ! aOpenInfo[ UR_OI_SHARED ]
        IF aCollData[ COLLDATA_OPENNUMBER ] == 1
            aCollData[ COLLDATA_LOCKED ] := .T.
        ELSE
            oError := ErrorNew()
            oError:GenCode     := EG_OPEN
            oError:SubCode     := 1000
            oError:Description := hb_langErrMsg( EG_OPEN ) + "(" + ;
                hb_langErrMsg( EG_LOCK ) + " - already opened in shared mode)"
            oError:FileName    := aOpenInfo[ UR_OI_NAME ]
            oError:CanDefault  := .T.
            UR_SUPER_ERROR( nWA, oError )
            NetErr( .T. )
            RETURN HB_FAILURE
        ENDIF
    ENDIF
 
    IF nResult == HB_SUCCESS
        NetErr( .F. )
        MDB_GOTOP( nWA )
    ENDIF
 
RETURN nResult

STATIC PROCEDURE mapIndexInfo(indexInfo, aStruct)
    LOCAL itm
    LOCAL a
    LOCAL n
    LOCAL s

    a := {}
    FOR EACH itm IN indexInfo["keys"]
        s := itm:__enumKey()
        n := AScan(aStruct, {|x| x[1] == s})
        AAdd(a, aStruct[n])
    NEXT

    indexInfo["fields"] := a

RETURN

STATIC FUNCTION MDB_ORDINFO( nWA, nMsg, aOrderInfo )

    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL aIndexes := aWAData[WADATA_COLLDATA][COLLDATA_INDEXARRAY]
    LOCAL nIndex
    LOCAL cursor
    LOCAL _id
    LOCAL ref
    LOCAL s

    HB_TRACE( HB_TR_DEBUG, hb_StrFormat( "nWA: %1$d, nMsg: %2$s, aOrderInfo: %3$s", nWA, hb_ValToExp( nMsg ), hb_ValToExp( aOrderInfo ) ) )
 
    IF Empty( aOrderInfo[ UR_ORI_TAG ] )
        aOrderInfo[ UR_ORI_TAG ] := aOrderInfo[ UR_ORI_BAG ]
    ENDIF

    SWITCH ValType( aOrderInfo[ UR_ORI_TAG ] )
        CASE "C"
            nIndex := Upper( aOrderInfo[ UR_ORI_TAG ] )
            nIndex := AScan( aIndexes, {| x | upper(x["name"]) == nIndex } )
            EXIT
        CASE "N"
            nIndex := aOrderInfo[ UR_ORI_TAG ]
            EXIT
            OTHERWISE
            nIndex := aWAData[ WADATA_INDEX ]
    ENDSWITCH
 
    SWITCH nMsg
        CASE DBOI_EXPRESSION
            s := ""
            aOrderInfo[UR_ORI_RESULT] := s
            EXIT
        CASE DBOI_ISDESC
            aOrderInfo[ UR_ORI_RESULT ] := .F.
            EXIT
        CASE DBOI_KEYCOUNT
            aOrderInfo[UR_ORI_RESULT] := ordKeyCount(nWA)
            EXIT
        CASE DBOI_KEYVAL
            IF aWAData[WADATA_EOF]
                aOrderInfo[UR_ORI_RESULT] := NIL
            ELSE
                aOrderInfo[UR_ORI_RESULT] := buildOrdKeyVal(aIndexes[nIndex], aWAData[WADATA_BSONDOC])
            ENDIF
            EXIT
        CASE DBOI_NUMBER
            aOrderInfo[UR_ORI_RESULT] := AScan(aIndexes, {|x| valType(aOrderInfo[UR_ORI_TAG]) == "C" .AND. x["name"] == upper(aOrderInfo[UR_ORI_TAG])})
            EXIT
        CASE DBOI_ORDERCOUNT
            aOrderInfo[UR_ORI_RESULT] := len(aIndexes)
            EXIT
        CASE DBOI_POSITION
            IF aWAData[WADATA_COLLDATA][COLLDATA_IDISOID]
                _id := {"_id" => {"$lte" => {"$oid" => aWAData[WADATA_RECNO]}}}
            ELSE
                _id := {"$lte" => aWAData[WADATA_RECNO]}
            ENDIF
            cursor := mongoc_collection_aggregate(aWAData[WADATA_COLLDATA][COLLDATA_COLLECTIONPTR], NIL, ;
                {;
                {"$match" => _id},;
                {"$count" => "total"};
                }; 
                )
            aOrderInfo[UR_ORI_RESULT] := 0
            WHILE mongoc_cursor_next(cursor, @ref)
                aOrderInfo[UR_ORI_RESULT] := hb_bson_value(ref, "total")
            ENDDO
            EXIT
        CASE DBOI_SCOPETOP
            aOrderInfo[ UR_ORI_RESULT ] := aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope0" ]
            IF aOrderInfo[ UR_ORI_ALLTAGS ] != NIL
                aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope0" ] := aOrderInfo[ UR_ORI_NEWVAL ]
            ENDIF
            EXIT
        CASE DBOI_SCOPETOPCLEAR
            aOrderInfo[ UR_ORI_RESULT ] := aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope0" ]
            IF aOrderInfo[ UR_ORI_ALLTAGS ] != NIL
                aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope0" ] := NIL
            ENDIF
            EXIT
        CASE DBOI_SCOPEBOTTOM
            aOrderInfo[ UR_ORI_RESULT ] := aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope1" ]
            IF aOrderInfo[ UR_ORI_ALLTAGS ] != NIL
                aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope1" ] := aOrderInfo[ UR_ORI_NEWVAL ]
            ENDIF
            EXIT
        CASE DBOI_SCOPEBOTTOMCLEAR
            aOrderInfo[ UR_ORI_RESULT ] := aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope1" ]
            IF aOrderInfo[ UR_ORI_ALLTAGS ] != NIL
                aWAData[ WADATA_WAORDINFO ][ nIndex ][ "scope1" ] := NIL
            ENDIF
            EXIT
            OTHERWISE
            outStd(e"\nnOperation not supported Msg =", nMsg)
            nIndex:notSupported()
            RETURN HB_FAILURE
    ENDSWITCH
 
RETURN HB_SUCCESS
 
STATIC FUNCTION MDB_ORDLSTFOCUS( nWA, aOrderInfo )
    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL aCollData := aWAData[ WADATA_COLLDATA ]
    LOCAL aIndexes := aCollData[ COLLDATA_INDEXARRAY ]
    LOCAL xIndex   := aOrderInfo[ UR_ORI_TAG ]
    LOCAL n
    LOCAL oError
    LOCAL failed

    aOrderInfo[ UR_ORI_RESULT ] := iif( aWAData[ WADATA_INDEX ] != 0, aIndexes[aWAData[WADATA_INDEX]]["name"], "" )

    SWITCH valType(xIndex)
        CASE "N"
            failed := xIndex != 0 .AND. xIndex > len(aIndexes)
            IF !failed
                aWAData[WADATA_INDEX] := xIndex
            ENDIF
            EXIT
        CASE "C"
            xIndex := upper(xIndex)
            n := AScan(aIndexes, {|x| upper(x["name"]) == xIndex})
            failed := !empty(xIndex) .AND. n == 0
            IF !failed
                aWAData[WADATA_INDEX] := n
            ENDIF
            EXIT
    ENDSWITCH

    IF failed == .T.
        oError := ErrorNew()
        oError:GenCode     := EG_OPEN
        oError:SubCode     := 1000
        oError:Description := "TAG index '" + xIndex + "' not exist on Collection '" + aCollData[COLLDATA_NAME] + "'"
        oError:FileName    := aCollData[COLLDATA_NAME]
        oError:CanDefault  := .T.
        UR_SUPER_ERROR( nWA, oError )
        NetErr( .T. )
        RETURN HB_FAILURE
    ENDIF
 
RETURN HB_SUCCESS

STATIC FUNCTION MDB_RECCOUNT( nWA, nRecords )
    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL aCollData := aWAData[ WADATA_COLLDATA ]
    nRecords := mongoc_collection_count_documents(aCollData[COLLDATA_COLLECTIONPTR], bson_new())
RETURN HB_SUCCESS
 
STATIC FUNCTION MDB_RECID( nWA, nRecNo )
    LOCAL aWAData := USRRDD_AREADATA( nWA )
 
    IF aWAData[ WADATA_EOF ]
        nRecNo := NIL
    ELSE
        nRecNo := aWAData[WADATA_RECNO]
    ENDIF
 
RETURN HB_SUCCESS
  
STATIC FUNCTION MDB_RECNO( nWA, nRecNo )
    LOCAL aWAData := USRRDD_AREADATA( nWA )
    LOCAL nResult := HB_SUCCESS

    nRecNo := aWAData[WADATA_RECNO]

RETURN nResult
 
STATIC FUNCTION MDB_SKIPFILTER( nWA, nRecords )

    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL lBof, nToSkip
 
    HB_TRACE( HB_TR_DEBUG, hb_StrFormat( "nWA: %1$d, nRecords: %2$d", nWA, nRecords ) )
 
    nToSkip := iif( nRecords > 0, 1, iif( nRecords < 0, -1, 0 ) )
 
    IF nToSkip != 0
        DO WHILE ! aWAData[ WADATA_BOF ] .AND. ! aWAData[ WADATA_EOF ]
            IF ( aWAData[ WADATA_FILTERINFO ] != NIL .AND. ! Eval( aWAData[ WADATA_FILTERINFO ][ UR_FRI_BEXPR ] ) )
                IF MDB_SKIPRAW( nWA, nToSkip ) != HB_SUCCESS
                    RETURN HB_FAILURE
                ENDIF
                IF nToSkip < 0 .AND. aWAData[ WADATA_BOF ]
                    lBof := .T.
                    aWAData[ WADATA_BOF ] := .F.
                    nToSkip := 1
                ELSEIF nToSkip > 0 .AND. aWAData[ WADATA_EOF ]
                    EXIT
                ENDIF
                LOOP
            ENDIF

            /* FILTERS */
            EXIT
        ENDDO
 
        IF lBof != NIL
            aWAData[ WADATA_BOF ] := .T.
        ENDIF
    ENDIF
 
RETURN HB_SUCCESS

STATIC FUNCTION getIdFromWAData(nWA)
    LOCAL bsonDoc := USRRDD_AREADATA( nWA )[WADATA_BSONDOC]
RETURN iif(bsonDoc == NIL, NIL, bsonDoc["_id"])

STATIC FUNCTION MDB_SEEK( nWa, lSoftSeek, xSeek, lLast )
    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL aIndexes := aWAData[ WADATA_COLLDATA ][ COLLDATA_INDEXARRAY ]
    LOCAL nIndex   := aWAData[ WADATA_INDEX ]
    LOCAL hDoc
    LOCAL indexState
    LOCAL indexStateSize

    indexState := getIndexState(nWa)
    indexStateSize := len(indexState)

    IF indexStateSize > 0 .AND. !hb_hHasKey(indexState[indexStateSize], nIndex)
        indexState[indexStateSize][nIndex] := hb_hClone(aWAData[WADATA_WAORDINFO][nIndex])
    ENDIF

    aWAData[WADATA_WAORDINFO][nIndex]["seek"]["key"] := xSeek
    aWAData[WADATA_WAORDINFO][nIndex]["seek"]["soft"] := lSoftSeek
    aWAData[WADATA_WAORDINFO][nIndex]["seek"]["last"] := lLast

    hDoc := Seek( nWA, xSeek, lSoftSeek, lLast, aIndexes[ nIndex ] )
    aWAData[ WADATA_ORDRECNO ] := hDoc

    IF hDoc == NIL
        aWAData[WADATA_FOUND] := .F.
        aWAData[WADATA_WAORDINFO][nIndex]["seek"]["skipped"] := NIL
    ELSE
        aWAData[WADATA_FOUND] := .T.
        aWAData[WADATA_WAORDINFO][nIndex]["seek"]["skipped"] := 0
    ENDIF

    setDbState(aWAData, hDoc)

RETURN HB_SUCCESS
 
STATIC FUNCTION MDB_SKIPRAW( nWA, nRecords )
    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL nIndex   := aWAData[ WADATA_INDEX ]
    LOCAL lBof, lEof
    LOCAL nResult
    LOCAL pipeline
    LOCAL _id := getIdFromWAData(nWA)
    LOCAL cursor
    LOCAL ref
    LOCAL hDoc
    LOCAL prtColl := aWAData[WADATA_COLLDATA][COLLDATA_COLLECTIONPTR]
    LOCAL xSeek, lSoftSeek, lLast, index, nToSkip
    LOCAL skipped
    LOCAL oid

    // outStd(e"\nSKIPRAW", "nIndex =", nIndex, "nRecords", nRecords)

    IF nRecords == 0
        lBof := aWAData[ WADATA_BOF ]
        lEof := aWAData[ WADATA_EOF ]

        oid := getIdFromWAData(nWA)

        IF oid != NIL
            pipeline := {}
            AAdd(pipeline, {"$match" => {"_id" => oid}})   
            cursor := mongoc_collection_aggregate(prtColl, NIL, pipeline)
            WHILE mongoc_cursor_next(cursor, @ref)
                hDoc := hb_bson_as_hash(ref)
            ENDDO
        ENDIF
     
        setDbState(aWAData, hDoc)

        aWAData[ WADATA_BOF ] := lBof
        aWAData[ WADATA_EOF ] := lEof

        nResult := HB_SUCCESS
    
    ELSEIF nIndex == 0
        pipeline := {}
        IF nRecords > 0
            AAdd(pipeline, {"$match" => {"_id" => {"$gte" => _id}}})
            AAdd(pipeline, {"$skip" => nRecords})
            AAdd(pipeline, {"$limit" => 1})
            cursor := mongoc_collection_aggregate(prtColl, NIL, pipeline)
            WHILE mongoc_cursor_next(cursor, @ref)
                hDoc := hb_bson_as_hash(ref)
            ENDDO
            nResult := MDB_GOTO(nWA, hDoc)
        ELSE
        ENDIF
    ELSEIF nIndex > 0
        xSeek := aWAData[WADATA_WAORDINFO][nIndex]["seek"]["key"]
        lSoftSeek := aWAData[WADATA_WAORDINFO][nIndex]["seek"]["soft"]
        lLast := aWAData[WADATA_WAORDINFO][nIndex]["seek"]["last"]
        index := aWAData[ WADATA_COLLDATA ][ COLLDATA_INDEXARRAY ][nIndex]
        skipped := aWAData[WADATA_WAORDINFO][nIndex]["seek"]["skipped"]
        IF skipped == NIL
            skipped := findCurrentIndexPos(aWAData, )
        ENDIF
        IF skipped == NIL
            skipped:failMiserably()
            RETURN HB_FAILURE
        ENDIF
        nToSkip := skipped + nRecords
        IF xSeek != NIL
            IF nToSkip < 0
                aWAData[ WADATA_BOF ] := .T.
                nResult := HB_FAILURE
            ELSE
                hDoc := Seek(nWA, xSeek, lSoftSeek, lLast, index, @nToSkip)
                IF hDoc != NIL
                    // outStd(e"\nSKIP ", ntoc(nToSkip), "->", hDoc["_id"]["$oid"], hDoc["tipo"], hDoc["gasWeight"], hDoc["family"], hDoc["descrip"])
                ENDIF
                aWAData[WADATA_WAORDINFO][nIndex]["seek"]["skipped"] := nToSkip
                setDbState(aWAData, hDoc)
                nResult := HB_SUCCESS
            ENDIF
        ELSE
            nWA:notImplemented()
        ENDIF
    ENDIF

RETURN nResult

STATIC FUNCTION ordKeyCount(nWA)
    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL collPtr := aWAData[WADATA_COLLDATA][COLLDATA_COLLECTIONPTR]
    LOCAL cursor
    LOCAL ref
    LOCAL hDoc

    cursor := mongoc_collection_aggregate(collPtr, NIL,;
        {;
        buildScopedMatchStage(aWAData),;
        {"$count" => "count"};
        };
        )
    WHILE mongoc_cursor_next(cursor, @ref)
        hDoc := hb_bson_as_hash(ref)
    ENDDO
    IF hDoc != NIL
        RETURN hDoc["count"]
    ENDIF

RETURN 0
     
STATIC FUNCTION SeekScope( nWA, aIndex, aOrdInfo, lBottom )
RETURN Seek( nWA, aOrdInfo["scope0"], .T., lBottom, aIndex )  

STATIC FUNCTION buildOrdKeyVal(hIndex, hDoc)
    LOCAL xResult
    LOCAL fldStruct
    LOCAL i

    FOR i := 1 TO len(hIndex["fields"])
        fldStruct := hIndex["fields"][i]
        SWITCH valType(fldStruct[2])
            CASE "C"
                IF xResult == NIL
                    xResult := ""
                ENDIF
                xResult += padR(hDoc[fldStruct[1]], fldStruct[3])
                EXIT
                OTHERWISE
                xResult:notImplemented()
                EXIT
        ENDSWITCH
    NEXT

RETURN xResult

STATIC FUNCTION buildMatchExpr(xSeek, indexInfo, lSoft, match)
    LOCAL itm
    LOCAL availBufferLen := len(xSeek)
    LOCAL fieldLen

    IF match == NIL
        match := {=>}
    ENDIF

    FOR EACH itm IN indexInfo["fields"]
        IF availBufferLen > 0
            fieldLen := itm[3]
            availBufferLen -= fieldLen
            IF lSoft == .T.
                match[itm[1]] := {"$gte" => left(xSeek, fieldLen):rTrim()}
            ELSE
                IF len(xSeek) < fieldLen
                    match[itm[1]] := {"$regex" => "^" + left(xSeek, fieldLen):rTrim()}
                ELSE
                    match[itm[1]] := left(xSeek, fieldLen):rTrim()
                ENDIF
            ENDIF
            IF availBufferLen > 0
                xSeek := subStr(xSeek, fieldLen + 1)
            ENDIF
        ENDIF
    NEXT

RETURN match

STATIC FUNCTION buildScopedMatchStage(aWAData, scope0, scope1)
    LOCAL nIndex := aWAData[WADATA_INDEX]
    LOCAL ordInfo := aWAData[WADATA_WAORDINFO][nIndex]
    LOCAL match := {=>}

    IF scope0 == NIL .AND. scope1 == NIL
        scope0 := ordInfo["scope0"]
        scope1 := ordInfo["scope1"]
    ENDIF

    IF scope0 != NIL .OR. scope1 != NIL
        IF scope0 == scope1
            buildMatchExpr(scope0, aWAData[WADATA_COLLDATA][COLLDATA_INDEXARRAY][nIndex], .F., match)
        ELSE
            IF scope0 != NIL
                buildMatchExpr(scope0, aWAData[WADATA_COLLDATA][COLLDATA_INDEXARRAY][nIndex], .T., match)
            ENDIF
            IF scope1 != NIL
                buildMatchExpr(scope1, aWAData[WADATA_COLLDATA][COLLDATA_INDEXARRAY][nIndex], .T., match)
            ENDIF
        ENDIF
    ENDIF

RETURN {"$match" => match}

STATIC FUNCTION Seek( nWA, xSeek, lSoft, lLast, indexInfo, nToSkip)
    LOCAL aWAData  := USRRDD_AREADATA( nWA )
    LOCAL ptrColl := aWAData[WADATA_COLLDATA][COLLDATA_COLLECTIONPTR]
    LOCAL pipeline := {}
    LOCAL cursor
    LOCAL ref, hDoc
    LOCAL itm
    LOCAL h1

    IF nToSkip == NIL
        nToSkip := 0
    ENDIF

    AAdd(pipeline, {"$match" => buildMatchExpr(xSeek, indexInfo, lSoft)})
    IF lLast == .T.
        h1 := {=>}
        FOR EACH itm IN indexInfo["keys"]
            h1[itm:__enumKey] := -itm
        NEXT
    ELSE
        AAdd(pipeline, {"$sort" => indexInfo["keys"]})
    ENDIF
    IF nToSkip > 0
        AAdd(pipeline, {"$skip" => nToSkip})
    ENDIF
    AAdd(pipeline, {"$limit" => 1})

    cursor := mongoc_collection_aggregate(ptrColl, NIL, hb_bson_append(NIL, pipeline))

    WHILE mongoc_cursor_next(cursor, @ref)
        hDoc := hb_bson_as_hash(ref)
    ENDDO

RETURN hDoc

FUNCTION MONGODBRDD_GETFUNCTABLE(pFuncCount, pFuncTable, pSuperTable, nRddID, pSuperRddID)
    local cSuperRDD := NIL
    LOCAL aFunc[UR_METHODCOUNT]

    s_nRddID := nRddID

    aFunc[ UR_INIT         ] := ( @MDB_INIT()         )
    aFunc[ UR_NEW          ] := ( @MDB_NEW()          )
    // aFunc[ UR_FLUSH        ] := ( @MDB_DUMMY()        )
    // aFunc[ UR_CREATE       ] := ( @MDB_CREATE()       )
    // aFunc[ UR_CREATEFIELDS ] := ( @MDB_CREATEFIELDS() )
    aFunc[ UR_OPEN         ] := ( @MDB_OPEN()         )
    // aFunc[ UR_CLOSE        ] := ( @MDB_CLOSE()        )
    aFunc[ UR_BOF          ] := ( @MDB_BOF()          )
    aFunc[ UR_EOF          ] := ( @MDB_EOF()          )
    // aFunc[ UR_APPEND       ] := ( @MDB_APPEND()       )
    // aFunc[ UR_DELETE       ] := ( @MDB_DELETE()       )
    aFunc[ UR_DELETED      ] := ( @MDB_DELETED()      )
    // aFunc[ UR_RECALL       ] := ( @MDB_RECALL()       )
    // aFunc[ UR_SETFILTER    ] := ( @MDB_SETFILTER()    )
    // aFunc[ UR_CLEARFILTER  ] := ( @MDB_CLEARFILTER()  )
    aFunc[ UR_SKIPFILTER   ] := ( @MDB_SKIPFILTER()   )
    aFunc[ UR_SKIPRAW      ] := ( @MDB_SKIPRAW()      )
    aFunc[ UR_GOTO         ] := ( @MDB_GOTO()         )
    aFunc[ UR_GOTOID       ] := ( @MDB_GOTOID()       )
    aFunc[ UR_GOTOP        ] := ( @MDB_GOTOP()        )
    aFunc[ UR_GOBOTTOM     ] := ( @MDB_GOBOTTOM()     )
    aFunc[ UR_RECNO        ] := ( @MDB_RECNO()        )
    aFunc[ UR_RECID        ] := ( @MDB_RECID()        )
    // aFunc[ UR_LOCK         ] := ( @MDB_LOCK()         )
    // aFunc[ UR_UNLOCK       ] := ( @MDB_UNLOCK()       )
    aFunc[ UR_RECCOUNT     ] := ( @MDB_RECCOUNT()     )
    aFunc[ UR_GETVALUE     ] := ( @MDB_GETVALUE()     )
    // aFunc[ UR_PUTVALUE     ] := ( @MDB_PUTVALUE()     )
    // aFunc[ UR_PACK         ] := ( @MDB_PACK()         )
    // aFunc[ UR_ZAP          ] := ( @MDB_ZAP()          )
    // aFunc[ UR_GOCOLD       ] := ( @MDB_GOCOLD()       )
    aFunc[ UR_FOUND        ] := ( @MDB_FOUND()        )
    aFunc[ UR_SEEK         ] := ( @MDB_SEEK()         )
    // aFunc[ UR_INFO         ] := ( @MDB_INFO()         )
    // aFunc[ UR_ORDLSTADD    ] := ( @MDB_ORDLSTADD()    )
    aFunc[ UR_ORDLSTFOCUS  ] := ( @MDB_ORDLSTFOCUS()  )
    // aFunc[ UR_ORDCREATE    ] := ( @MDB_ORDCREATE()    )
    aFunc[ UR_ORDINFO      ] := ( @MDB_ORDINFO()      )
    // aFunc[ UR_CLEARLOCATE  ] := ( @MDB_CLEARLOCATE()  )
    // aFunc[ UR_SETLOCATE    ] := ( @MDB_SETLOCATE()    )
    // aFunc[ UR_LOCATE       ] := ( @MDB_LOCATE()       )
 
RETURN USRRDD_GETFUNCTABLE(pFuncCount, pFuncTable, pSuperTable, nRddID, cSuperRDD, aFunc, pSuperRddID)

STATIC FUNCTION getBrowseStateMap(nWA)
    IF !hb_hHasKey(browseStateMap, iif(nWA == NIL, select(), nWA))
        browseStateMap[nWA] := {"stack" => {}, "stackSize" => 0}
    ENDIF
RETURN browseStateMap[nWA]

STATIC FUNCTION getIndexState(nWA)
    IF !hb_hHasKey(indexStateMap, nWA)
        indexStateMap[nWA] := {}
    ENDIF
RETURN indexStateMap[nWA]

PROCEDURE pushIndexState()
    LOCAL nWA
    LOCAL indexState
    IF rddName() != "MONGODBRDD"
        RETURN
    ENDIF
    nWA := select()
    indexState := getIndexState(nWA)
    AAdd(indexState, {=>})
RETURN

PROCEDURE popIndexState()
    LOCAL nWA
    LOCAL indexState
    LOCAL indexStateSize
    LOCAL aWAData
    LOCAL itm
    IF rddName() != "MONGODBRDD"
        RETURN
    ENDIF
    nWA := select()
    indexState := getIndexState(nWA)
    indexStateSize := len(indexState)
    IF indexStateSize > 0
        aWAData  := USRRDD_AREADATA( nWA )
        FOR EACH itm IN indexState[indexStateSize]
            aWAData[WADATA_WAORDINFO][itm:__enumKey] := itm
        NEXT
    ENDIF
    hb_ADel(indexState, indexStateSize, .T.)
RETURN

PROCEDURE pushBrowseState()
    LOCAL nWA
    LOCAL browseState
    LOCAL aWAData
    LOCAL nIndex
    LOCAL iSeek

    IF rddName() != "MONGODBRDD"
        RETURN
    ENDIF

    nWA := select()
    browseState := getBrowseStateMap(nWA)
    aWAData  := USRRDD_AREADATA( nWA )
    nIndex := ordNumber(ordSetFocus())

    ++browseState["stackSize"]
    IF len(browseState["stack"]) < browseState["stackSize"]
        AAdd(browseState["stack"], {=>})
    ENDIF
    IF nIndex > 0
        // outStd(e"\npushBrowseState:", hb_jsonEncode(aWAData[WADATA_WAORDINFO][nIndex]["seek"]), "nIndex", nIndex)
        iSeek := aWAData[WADATA_WAORDINFO][nIndex]["seek"]
        browseState["stack"][browseState["stackSize"]]["state"] := hb_hClone(iSeek)
        browseState["stack"][browseState["stackSize"]]["nIndex"] := nIndex
    ENDIF
RETURN

PROCEDURE popBrowseState(keepInStack)
    LOCAL nWA
    LOCAL browseState
    LOCAL aWAData
    LOCAL nIndex

    IF rddName() != "MONGODBRDD"
        RETURN
    ENDIF

    nWA := select()
    browseState := getBrowseStateMap(nWA)
    aWAData  := USRRDD_AREADATA( nWA )
    // outStd(e"\nbrowseState =", hb_jsonEncode(browseState))
    nIndex := browseState["stack"][browseState["stackSize"]]["nIndex"]

    IF nIndex > 0
        aWAData[WADATA_WAORDINFO][nIndex]["seek"] := hb_hClone(browseState["stack"][browseState["stackSize"]]["state"])
    ENDIF
    IF !keepInStack == .T.
        outStd(e"\npopBrowseState:", hb_jsonEncode(browseState["stack"][browseState["stackSize"]]["state"]))
        --browseState["stackSize"]
    ENDIF
RETURN

INIT PROCEDURE MONGODBRRDD_INIT()
    rddRegister("MONGODBRDD", RDT_FULL)
RETURN

PROCEDURE test() 
    LOCAL aWAData
    LOCAL pColl
    LOCAL t
    LOCAL pipeline
    LOCAL cursor
    LOCAL ref
    LOCAL n
    LOCAL counter
    LOCAL oid

    // this is the way

    dbSelectArea("MAIN")

    IF alias() != "MAIN"
        RETURN
    ENDIF
    aWAData  := USRRDD_AREADATA( select() )    

    pColl := aWAData[WADATA_COLLDATA][COLLDATA_COLLECTIONPTR]
    pipeline := {}
    AAdd(pipeline, {"$match" => {"family" => "202"}})
    AAdd(pipeline, {"$project" => {"family" => 1, "tipo" => 1, "gasWeight" => 1, "descrip" => 1}})
    n := 0
    bson_oid_init_from_string(@oid, "640fa2f4b5c744b4c728d426")
    t := hb_milliseconds()
    cursor := mongoc_collection_aggregate(pColl, NIL, pipeline)
    WHILE mongoc_cursor_next(cursor, @ref)
        IF bson_oid_equal(oid, hb_bson_value(ref, "_id"))
            counter := n
            EXIT
        ENDIF
        ++n
    ENDDO

    outStd(e"\ncounter =", counter, "millis =", hb_milliseconds() - t)

RETURN

STATIC FUNCTION findCurrentIndexPos(aWAData)
    LOCAL nIndex := aWAData[WADATA_INDEX]
    LOCAL indexInfo := aWAData[WADATA_COLLDATA][COLLDATA_INDEXARRAY][nIndex]
    LOCAL matchDoc
    LOCAL cursor
    LOCAL ref
    LOCAL skipped := NIL
    LOCAL recNo := aWAData[WADATA_RECNO]
    LOCAL oid
    LOCAL seekKey := aWAData[WADATA_WAORDINFO][nIndex]["seek"]["key"]
    LOCAL pipeline := {}

    bson_oid_init_from_string(@oid, recNo)

    matchDoc := buildScopedMatchStage(aWAData, seekKey, seekKey)

    AAdd(pipeline, matchDoc)
    AAdd(pipeline, {"$sort" => indexInfo["keys"]})

    cursor := mongoc_collection_aggregate(aWAData[WADATA_COLLDATA][COLLDATA_COLLECTIONPTR], NIL, {matchDoc})

    WHILE mongoc_cursor_next(cursor, @ref)
        IF skipped == NIL
            skipped := 0
        ELSE
            ++skipped
        ENDIF
        IF bson_oid_equal(oid, hb_bson_value(ref, "_id"))
            EXIT
        ENDIF
    ENDDO

RETURN skipped

FUNCTION isMongoDb()
RETURN rddName() == "MONGODBRDD"
