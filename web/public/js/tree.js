DocsViewer.TreePanel = function() {
    DocsViewer.TreePanel.superclass.constructor.call(this, {
        id:'docs-tree',
        region:'west',
        title:'Documentation',
        split:true,
        width: 225,
        minSize: 175,
        maxSize: 400,
        collapsible: false,
        margins:'5 0 5 5',
        cmargins:'5 5 5 5',
        lines:false,
        autoScroll:true,
        loader: new Ext.tree.TreeLoader({
            url: '/get_tree_level',
            preloadChildren: true,
            clearOnLoad: true
        }),
        root: new Ext.tree.AsyncTreeNode({
            text:'Documentation',
            id:'root',
            expanded:true,
            leaf: false
        }),
        collapseFirst:false
    });
};

Ext.extend(DocsViewer.TreePanel, Ext.tree.TreePanel, {});
