Semantix.dv.TreePanel = function() {
    Semantix.dv.TreePanel.superclass.constructor.call(this, {
        id:'semantix-dv-docs-tree',
        title:'Documentation',
        lines:false,
        autoScroll:true,
        rootVisible: false,
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
        collapseFirst:false,

        listeners: {
            render: function(tree) {
                var dd_group = 'semantix-dv-dd-documents-tree';

                tree.dragZone = new Semantix.ext.TreeDragZone(tree, {
                    ddGroup: dd_group
                });
                tree.enableDrag = true;
            }
        }
    });

    this.on('click', this.onClick, this);
};

Ext.extend(Semantix.dv.TreePanel, Ext.tree.TreePanel, {
    onClick : function(node, e) {
        e.stopEvent();
        Semantix.Bubbling.fire('semantix.dv.topics.selected', node.id);
    }
});
