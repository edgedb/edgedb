Semantix.dv.BookmarksPanel = function() {
    Semantix.dv.BookmarksPanel.superclass.constructor.call(this, {
        id:'semantix-dv-bookmarks-tree',
        title:'Bookmarks',
        lines:false,
        autoScroll:true,
        rootVisible: false,

        loader: new Ext.tree.TreeLoader({
            url: '/get_tree_level',
            preloadChildren: true,
            clearOnLoad: true
        }),

        root: new Ext.tree.AsyncTreeNode({
            text:'Bookmarks',
            id:'root',
            expanded:true,
            leaf: false
        }),
        collapseFirst:false
    });

    Semantix.Bubbling.on('semantix.dv.topics.add-bookmark', function(id) {
        Semantix.msg('added');
    });
};

Ext.extend(Semantix.dv.BookmarksPanel, Ext.tree.TreePanel, {

});
