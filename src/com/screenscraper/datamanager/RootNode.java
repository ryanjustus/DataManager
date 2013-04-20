/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.screenscraper.datamanager;
import java.util.*;
/**
 * The root node of the tree that stores all the DataNodes in the DataManager
 * @author ryan
 */
public class RootNode {
    private SortedSet<DataNode> children;
    public RootNode()
    {
        children = new TreeSet<DataNode>();
    }
    public Set<DataNode> getChildren()
    {
        return children;
    }
    public void addChild(DataNode n)
    {
        children.add(n);
    }
    public void removeChild(DataNode n)
    {
        children.remove(n);
    }

     /**
     * returns all the descendents of the current node with the name table
     * @param table
     * @return Set of DataNode containing the descendents
     */
    public SortedSet<DataNode> getChildren(String table)
    {

        SortedSet<DataNode> found = new TreeSet<DataNode>();
        Set<DataNode> visited = new HashSet<DataNode>();
        for(DataNode child: children)
        {
            getChildren(table, child, found, visited);
        }
        return found;
    }

    private void getChildren(String table, DataNode currentNode, SortedSet<DataNode> found, Set<DataNode> visited)
    {
       if(visited.contains(currentNode))
           return;
       visited.add(currentNode);
       if(currentNode.getSchema().getName().equals(table))
           found.add(currentNode);
       for(DataNode child: currentNode.getChildren())
       {
           getChildren(table, child, found, visited);
       }
    }
}
