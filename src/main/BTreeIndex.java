package main;

import java.util.ArrayList;
import java.util.List;

public class BTreeIndex<K extends Comparable<K>, V> {
    private static final int DEFAULT_ORDER = 3;

    private Node root;
    private int order;
    private int size;

    private class Node {
        List<Entry> entries;
        List<Node> children;
        boolean isLeaf;

        Node(boolean isLeaf) {
            this.isLeaf = isLeaf;
            this.entries = new ArrayList<>();
            this.children = new ArrayList<>();
        }
    }

    private class Entry {
        K key;
        V value;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public BTreeIndex() {
        this(DEFAULT_ORDER);
    }

    public BTreeIndex(int order) {
        this.order = order;
        this.root = new Node(true);
        this.size = 0;
    }

    public void put(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("键不能为 null");
        }

        if (root.entries.size() == order - 1) {
            Node newRoot = new Node(false);
            newRoot.children.add(root);
            splitChild(newRoot, 0);
            root = newRoot;
        }

        insertNonFull(root, key, value);
        size++;
    }

    private void insertNonFull(Node node, K key, V value) {
        int i = node.entries.size() - 1;

        if (node.isLeaf) {
            while (i >= 0 && key.compareTo(node.entries.get(i).key) < 0) {
                i--;
            }
            node.entries.add(i + 1, new Entry(key, value));
        } else {
            while (i >= 0 && key.compareTo(node.entries.get(i).key) < 0) {
                i--;
            }
            i++;

            if (node.children.get(i).entries.size() == order - 1) {
                splitChild(node, i);
                if (key.compareTo(node.entries.get(i).key) > 0) {
                    i++;
                }
            }

            insertNonFull(node.children.get(i), key, value);
        }
    }

    private void splitChild(Node parent, int index) {
        Node child = parent.children.get(index);
        Node newChild = new Node(child.isLeaf);

        int mid = (order - 1) / 2;
        for (int i = mid + 1; i < child.entries.size(); i++) {
            newChild.entries.add(child.entries.get(i));
        }
        child.entries.subList(mid + 1, child.entries.size()).clear();

        if (!child.isLeaf) {
            for (int i = mid + 1; i < child.children.size(); i++) {
                newChild.children.add(child.children.get(i));
            }
            child.children.subList(mid + 1, child.children.size()).clear();
        }

        Entry midEntry = child.entries.get(mid);
        child.entries.remove(mid);
        parent.entries.add(index, midEntry);
        parent.children.add(index + 1, newChild);
    }

    public V get(K key) {
        if (key == null) {
            return null;
        }

        Entry entry = search(root, key);
        return entry != null ? entry.value : null;
    }

    private Entry search(Node node, K key) {
        int i = 0;
        while (i < node.entries.size() && key.compareTo(node.entries.get(i).key) > 0) {
            i++;
        }

        if (i < node.entries.size() && key.equals(node.entries.get(i).key)) {
            return node.entries.get(i);
        }

        if (node.isLeaf) {
            return null;
        }

        return search(node.children.get(i), key);
    }

    public List<V> rangeQuery(K minKey, K maxKey) {
        List<V> results = new ArrayList<>();
        if (minKey == null || maxKey == null || minKey.compareTo(maxKey) > 0) {
            return results;
        }

        rangeQuery(root, minKey, maxKey, results);
        return results;
    }

    private void rangeQuery(Node node, K minKey, K maxKey, List<V> results) {
        int i = 0;

        while (i < node.entries.size() && minKey.compareTo(node.entries.get(i).key) > 0) {
            i++;
        }

        if (!node.isLeaf) {
            rangeQuery(node.children.get(i), minKey, maxKey, results);
        }

        while (i < node.entries.size() && node.entries.get(i).key.compareTo(maxKey) <= 0) {
            Entry entry = node.entries.get(i);
            if (entry.key.compareTo(minKey) >= 0 && entry.key.compareTo(maxKey) <= 0) {
                results.add(entry.value);
            }

            if (!node.isLeaf && i < node.children.size() - 1) {
                rangeQuery(node.children.get(i + 1), minKey, maxKey, results);
            }

            i++;
        }
    }

    public boolean containsKey(K key) {
        return get(key) != null;
    }

    public int size() {
        return size;
    }

    public void clear() {
        root = new Node(true);
        size = 0;
    }
}