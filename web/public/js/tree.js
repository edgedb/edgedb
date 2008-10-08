DocsViewer.TreePanel = function() {
    DocsViewer.TreePanel.superclass.constructor.call(this, {
        id:'docs-tree',
        region:'west',
        title:'Documentation',
        split:true,
        width: 350,
        minSize: 175,
        maxSize: 600,
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

    this.addEvents({topicselect:true});
    this.on('click', this.onClick, this);
};

Ext.extend(DocsViewer.TreePanel, Ext.tree.TreePanel, {
    onClick : function(node) {
        this.fireEvent('topicselect', node.id);
    }
});
