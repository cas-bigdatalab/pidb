/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tooling;

import org.neo4j.tooling.ImportTool;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import org.neo4j.csv.reader.IllegalMultilineFieldException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.util.Validator;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.internal.Version;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.DuplicateInputIdException;
import org.neo4j.unsafe.impl.batchimport.input.InputException;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.repeat;
//import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.helpers.ArrayUtil.join;
import static org.neo4j.helpers.Exceptions.contains;
import static org.neo4j.helpers.Exceptions.withMessage;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.count;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.fs.FileUtils.writeToFile;
import static org.neo4j.kernel.impl.store.MetaDataStore.DEFAULT_NAME;
import static org.neo4j.tooling.ImportTool.MULTI_FILE_DELIMITER;
import static org.neo4j.unsafe.impl.batchimport.Configuration.BAD_FILE_NAME;

public class ImportToolTest
{
    private static final int MAX_LABEL_ID = 4;
    private static final int RELATIONSHIP_COUNT = 10_000;
    private static final int NODE_COUNT = 100;
    private static final IntPredicate TRUE = i -> true;

    @Rule
    public final EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule().startLazily();
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppress( SuppressOutput.System.values() );
    private int dataIndex;



    @Test
    public void import4097Labels() throws Exception
    {
        System.out.println("bbb");

        // GIVEN
        File header = file(  "test-header.csv"  );
//        try ( PrintStream writer = new PrintStream( header )  )
//        {
//            writer.println( ":LABEL" );
//        }
        File data = file( "test.csv"  );
//        try ( PrintStream writer = new PrintStream( data ) )
//        {
//            // Need to have unique names in order to get unique ids for labels. Want 4096 unique label ids present.
//            for ( int i = 0; i < 4096; i++ )
//            {
//                writer.println( "SIMPLE" + i );
//            }
//            // Then insert one with 3 array entries which will get ids greater than 4096. These cannot be inlined
//            // due 36 bits being divided into 3 parts of 12 bits each and 4097 > 2^12, thus these labels will be
//            // need to be dynamic records.
//            writer.println( "FIRST 4096|SECOND 4096|THIRD 4096" );
//        }

        // WHEN
        importTool( "--into", dbRule.getStoreDirAbsolutePath(),
//                "--delimiter", ",",
                "--additional-config", "neo4j.conf",
                "--array-delimiter", "|",
                "--nodes", header.getAbsolutePath() + MULTI_FILE_DELIMITER + data.getAbsolutePath() );

        // THEN
        try ( Transaction tx = dbRule.beginTx() )
        {
            long nodeCount = Iterables.count( dbRule.getAllNodes() );
            System.out.println("XXXX:"+nodeCount);
            assertEquals( 3, nodeCount );

            tx.success();
            ResourceIterator<Node> nodes = dbRule.findNodes(label("Movie"));
            assertEquals( 1, Iterators.asList( nodes ).size() );
        }
    }


    private void assertContains( List<String> errorLines, String string )
    {
        for ( String line : errorLines )
        {
            if ( line.contains( string ) )
            {
                return;
            }
        }
        fail( "Expected error lines " + join( errorLines.toArray( new String[errorLines.size()] ), format( "%n" ) ) +
                " to have at least one line containing the string '" + string + "'" );
    }

    private static int occurencesOf( String text, String lookFor )
    {
        int index = -1;
        int count = -1;
        do
        {
            count++;
            index = text.indexOf( lookFor, index + 1 );
        }
        while ( index != -1 );
        return count;
    }

    private File writeArrayCsv( String[] headers, String[] values ) throws FileNotFoundException
    {
        File data = file( fileName( "whitespace.csv" ) );
        try ( PrintStream writer = new PrintStream( data ) )
        {
            writer.print( ":LABEL" );
            for ( String header : headers )
            {
                writer.print( "," + header );
            }
            // End line
            writer.println();

            // Save value as a String in name
            writer.print( "PERSON" );
            // For each type
            for ( String ignored : headers )
            {
                boolean comma = true;
                for ( String value : values )
                {
                    if ( comma )
                    {
                        writer.print( "," );
                        comma = false;
                    }
                    else
                    {
                        writer.print( ";" );
                    }
                    writer.print( value );
                }
            }
            // End line
            writer.println();
        }
        return data;
    }

    private String joinStringArray( String[] values )
    {
        return Arrays.stream( values ).map( String::trim ).collect( joining( ", ", "[", "]" ) );
    }

    private File data( String... lines ) throws Exception
    {
        File file = file( fileName( "data.csv" ) );
        try ( PrintStream writer = writer( file, Charset.defaultCharset() ) )
        {
            for ( String line : lines )
            {
                writer.println( line );
            }
        }
        return file;
    }

    private Predicate<Node> nodeFilter( final String id )
    {
        return node -> node.getProperty( "id", "" ).equals( id );
    }

    private void assertNodeHasLabels( Node node, String[] names )
    {
        for ( String name : names )
        {
            assertTrue( node + " didn't have label " + name + ", it had labels " + node.getLabels(),
                    node.hasLabel( label( name ) ) );
        }
    }

    private void verifyData()
    {
        verifyData( Validators.emptyValidator(), Validators.emptyValidator() );
    }

    private void verifyData(
            Validator<Node> nodeAdditionalValidation,
            Validator<Relationship> relationshipAdditionalValidation )
    {
        verifyData( NODE_COUNT, RELATIONSHIP_COUNT, nodeAdditionalValidation, relationshipAdditionalValidation );
    }

    private void verifyData( int expectedNodeCount, int expectedRelationshipCount,
            Validator<Node> nodeAdditionalValidation,
            Validator<Relationship> relationshipAdditionalValidation )
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0;
            int relationshipCount = 0;
            for ( Node node : db.getAllNodes() )
            {
                assertTrue( node.hasProperty( "name" ) );
                nodeAdditionalValidation.validate( node );
                nodeCount++;
            }
            assertEquals( expectedNodeCount, nodeCount );
            for ( Relationship relationship : db.getAllRelationships() )
            {
                assertTrue( relationship.hasProperty( "created" ) );
                relationshipAdditionalValidation.validate( relationship );
                relationshipCount++;
            }
            assertEquals( expectedRelationshipCount, relationshipCount );
            tx.success();
        }
    }

    private void verifyRelationships( List<RelationshipDataLine> relationships )
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        Map<String,Node> nodesById = allNodesById( db );
        try ( Transaction tx = db.beginTx() )
        {
            for ( RelationshipDataLine relationship : relationships )
            {
                Node startNode = nodesById.get( relationship.startNodeId );
                Node endNode = nodesById.get( relationship.endNodeId );
                if ( startNode == null || endNode == null )
                {
                    // OK this is a relationship refering to a missing node, skip it
                    continue;
                }
                assertNotNull( relationship.toString(), findRelationship( startNode, endNode, relationship ) );
            }
            tx.success();
        }
    }

    private Relationship findRelationship( Node startNode, final Node endNode, final RelationshipDataLine relationship )
    {
        return Iterators.singleOrNull( Iterators.filter(
                item -> item.getEndNode().equals( endNode ) &&
                        item.getProperty( "name" ).equals( relationship.name ),
                startNode.getRelationships( withName( relationship.type ) ).iterator() ) );
    }

    private Map<String,Node> allNodesById( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Map<String,Node> nodes = new HashMap<>();
            for ( Node node : db.getAllNodes() )
            {
                nodes.put( idOf( node ), node );
            }
            tx.success();
            return nodes;
        }
    }

    private String idOf( Node node )
    {
        return (String) node.getProperty( "id" );
    }

    private List<String> nodeIds()
    {
        return nodeIds( NODE_COUNT );
    }

    private List<String> nodeIds( int count )
    {
        List<String> ids = new ArrayList<>();
        for ( int i = 0; i < count; i++ )
        {
            ids.add( randomNodeId() );
        }
        return ids;
    }

    private String randomNodeId()
    {
        return UUID.randomUUID().toString();
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate ) throws Exception
    {
        return nodeData( includeHeader, config, nodeIds, linePredicate, Charset.defaultCharset() );
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, Charset encoding ) throws Exception
    {
        return nodeData( includeHeader, config, nodeIds, linePredicate, encoding, 0 );
    }

    private File nodeData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, Charset encoding, int extraColumns ) throws Exception
    {
        File file = file( fileName( "nodes.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            if ( includeHeader )
            {
                writeNodeHeader( writer, config, null );
            }
            writeNodeData( writer, config, nodeIds, linePredicate, extraColumns );
        }
        return file;
    }

    private PrintStream writer( File file, Charset encoding ) throws Exception
    {
        return new PrintStream( file, encoding.name() );
    }

    private File nodeHeader( Configuration config ) throws Exception
    {
        return nodeHeader( config, null );
    }

    private File nodeHeader( Configuration config, String idGroup ) throws Exception
    {
        return nodeHeader( config, idGroup, Charset.defaultCharset() );
    }

    private File nodeHeader( Configuration config, String idGroup, Charset encoding ) throws Exception
    {
        File file = file( fileName( "nodes-header.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            writeNodeHeader( writer, config, idGroup );
        }
        return file;
    }

    private void writeNodeHeader( PrintStream writer, Configuration config, String idGroup )
    {
        char delimiter = config.delimiter();
        writer.println( idEntry( "id", Type.ID, idGroup ) + delimiter + "name" + delimiter + "labels:LABEL" );
    }

    private String idEntry( String name, Type type, String idGroup )
    {
        return (name != null ? name : "") + ":" + type.name() + (idGroup != null ? "(" + idGroup + ")" : "");
    }

    private void writeNodeData( PrintStream writer, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, int extraColumns )
    {
        char delimiter = config.delimiter();
        char arrayDelimiter = config.arrayDelimiter();
        for ( int i = 0; i < nodeIds.size(); i++ )
        {
            if ( linePredicate.test( i ) )
            {
                writer.println( getLine( nodeIds.get( i ), delimiter, arrayDelimiter, extraColumns ) );
            }
        }
    }

    private String getLine( String nodeId, char delimiter, char arrayDelimiter, int extraColumns )
    {
        StringBuilder stringBuilder = new StringBuilder().append( nodeId ).append( delimiter ).append( randomName() )
                .append( delimiter ).append( randomLabels( arrayDelimiter ) );

        for ( int i = 0; i < extraColumns; i++ )
        {
            stringBuilder.append( delimiter ).append( "ExtraColumn" ).append( i );
        }

        return stringBuilder.toString();
    }

    private String randomLabels( char arrayDelimiter )
    {
        int length = random.nextInt( 3 );
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( arrayDelimiter );
            }
            builder.append( labelName( random.nextInt( MAX_LABEL_ID ) ) );
        }
        return builder.toString();
    }

    private String labelName( int number )
    {
        return "LABEL_" + number;
    }

    private String randomName()
    {
        int length = random.nextInt( 10 ) + 5;
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < length; i++ )
        {
            builder.append( (char) ('a' + random.nextInt( 20 )) );
        }
        return builder.toString();
    }

    private File relationshipData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, boolean specifyType ) throws Exception
    {
        return relationshipData( includeHeader, config, nodeIds, linePredicate, specifyType, Charset.defaultCharset() );
    }

    private File relationshipData( boolean includeHeader, Configuration config, List<String> nodeIds,
            IntPredicate linePredicate, boolean specifyType, Charset encoding ) throws Exception
    {
        return relationshipData( includeHeader, config, randomRelationships( nodeIds ), linePredicate,
                specifyType, encoding );
    }

    private File relationshipData( boolean includeHeader, Configuration config,
            Iterator<RelationshipDataLine> data, IntPredicate linePredicate,
            boolean specifyType ) throws Exception
    {
        return relationshipData( includeHeader, config, data, linePredicate, specifyType, Charset.defaultCharset() );
    }

    private File relationshipData( boolean includeHeader, Configuration config,
            Iterator<RelationshipDataLine> data, IntPredicate linePredicate,
            boolean specifyType, Charset encoding ) throws Exception
    {
        File file = file( fileName( "relationships.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            if ( includeHeader )
            {
                writeRelationshipHeader( writer, config, null, null, specifyType );
            }
            writeRelationshipData( writer, config, data, linePredicate, specifyType );
        }
        return file;
    }

    private File relationshipHeader( Configuration config ) throws Exception
    {
        return relationshipHeader( config, Charset.defaultCharset() );
    }

    private File relationshipHeader( Configuration config, Charset encoding ) throws Exception
    {
        return relationshipHeader( config, null, null, true, encoding );
    }

    private File relationshipHeader( Configuration config, String startIdGroup, String endIdGroup, boolean specifyType )
            throws Exception
    {
        return relationshipHeader( config, startIdGroup, endIdGroup, specifyType, Charset.defaultCharset() );
    }

    private File relationshipHeader( Configuration config, String startIdGroup, String endIdGroup, boolean specifyType,
            Charset encoding ) throws Exception
    {
        File file = file( fileName( "relationships-header.csv" ) );
        try ( PrintStream writer = writer( file, encoding ) )
        {
            writeRelationshipHeader( writer, config, startIdGroup, endIdGroup, specifyType );
        }
        return file;
    }

    private String fileName( String name )
    {
        return dataIndex++ + "-" + name;
    }

    private File file( String localname )
    {
//        return new File( dbRule.getStoreDir(), localname );
        return new File( localname );
    }

    private File badFile()
    {
        return new File( dbRule.getStoreDirFile(), BAD_FILE_NAME );
    }

    private void writeRelationshipHeader( PrintStream writer, Configuration config,
            String startIdGroup, String endIdGroup, boolean specifyType )
    {
        char delimiter = config.delimiter();
        writer.println(
                idEntry( null, Type.START_ID, startIdGroup ) + delimiter +
                idEntry( null, Type.END_ID, endIdGroup ) +
                (specifyType ? (delimiter + ":" + Type.TYPE) : "") +
                delimiter + "created:long" +
                delimiter + "name:String" );
    }

    private static class RelationshipDataLine
    {
        private final String startNodeId;
        private final String endNodeId;
        private final String type;
        private final String name;

        RelationshipDataLine( String startNodeId, String endNodeId, String type, String name )
        {
            this.startNodeId = startNodeId;
            this.endNodeId = endNodeId;
            this.type = type;
            this.name = name;
        }

        @Override
        public String toString()
        {
            return "RelationshipDataLine [startNodeId=" + startNodeId + ", endNodeId=" + endNodeId + ", type=" + type
                   + ", name=" + name + "]";
        }
    }

    private static RelationshipDataLine relationship( String startNodeId, String endNodeId, String type )
    {
        return relationship( startNodeId, endNodeId, type, null );
    }

    private static RelationshipDataLine relationship( String startNodeId, String endNodeId, String type, String name )
    {
        return new RelationshipDataLine( startNodeId, endNodeId, type, name );
    }

    private void writeRelationshipData( PrintStream writer, Configuration config,
            Iterator<RelationshipDataLine> data, IntPredicate linePredicate, boolean specifyType )
    {
        char delimiter = config.delimiter();
        for ( int i = 0; i < RELATIONSHIP_COUNT; i++ )
        {
            if ( !data.hasNext() )
            {
                break;
            }
            RelationshipDataLine entry = data.next();
            if ( linePredicate.test( i ) )
            {
                writer.println( nullSafeString( entry.startNodeId ) +
                                delimiter + nullSafeString( entry.endNodeId ) +
                                (specifyType ? (delimiter + nullSafeString( entry.type )) : "") +
                                delimiter + currentTimeMillis() +
                                delimiter + (entry.name != null ? entry.name : "")
                );
            }
        }
    }

    private static String nullSafeString( String endNodeId )
    {
        return endNodeId != null ? endNodeId : "";
    }

    private Iterator<RelationshipDataLine> randomRelationships( final List<String> nodeIds )
    {
        return new PrefetchingIterator<RelationshipDataLine>()
        {
            @Override
            protected RelationshipDataLine fetchNextOrNull()
            {
                return new RelationshipDataLine(
                        nodeIds.get( random.nextInt( nodeIds.size() ) ),
                        nodeIds.get( random.nextInt( nodeIds.size() ) ),
                        randomType(),
                        null );
            }
        };
    }

    static void assertExceptionContains( Exception e, String message, Class<? extends Exception> type )
            throws Exception
    {
        if ( !contains( e, message, type ) )
        {   // Rethrow the exception since we'd like to see what it was instead
            throw withMessage( e,
                    format( "Expected exception to contain cause '%s', %s. but was %s", message, type, e ) );
        }
    }

    private String randomType()
    {
        return "TYPE_" + random.nextInt( 4 );
    }

    private IntPredicate lines( final int startingAt, final int endingAt /*excluded*/ )
    {
        return line -> line >= startingAt && line < endingAt;
    }

    static void importTool( String... arguments ) throws IOException
    {
        ImportTool.main( arguments, true );
    }
}
