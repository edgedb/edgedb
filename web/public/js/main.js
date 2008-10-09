
Ext.onReady(function(){
    Ext.History.init();

    var tab_panel = new Ext.ux.TabPanel({
        id: 'semantix-dv-tree-tabs',

        activeTab: 0,

        region:'west',
        split:true,
        width: 350,
        minSize: 175,
        maxSize: 600,

        collapsible: false,

        margins:'5 0 5 5',
        cmargins:'5 5 5 5',

        items: [
            new Semantix.dv.TreePanel(),
            new Semantix.dv.BookmarksPanel()
        ],

        listeners: {
            render: initializeTopicDropZone
        }
    });

    var panel = new Semantix.dv.MainPanel({
        region: 'center',
        margins:'5 5 5 0',
        cmargins:'5 5 5 5'
      //  listeners: Semantix.dv.LinkInterceptor,
    });

    Ext.History.on('change', function(id){
        if(id) {
            Semantix.Bubbling.fire('semantix.dv.topics.selected', id);
        }
    });

    var viewport = new Ext.Viewport({
        layout : 'border',
        items: [
            tab_panel,
            panel
        ]
    });
});

function initializeTopicDropZone(g) {
    var dd_group = 'semantix-dv-dd-documents-tree';

    var bm_tree = Ext.getCmp('semantix-dv-bookmarks-tree');
    var bm_tree_tab = bm_tree._tab_button;
    var bm_tree_tab_id = bm_tree_tab.id;

    Semantix.Bubbling.on('semantix.dv.highlight-ddzone.on', function(zone) {
        if (zone != dd_group) {
            return;
        }

        (bm_tree.isVisible() ? bm_tree.body : bm_tree_tab).addClass('semantix-drop-zone');
    });

    Semantix.Bubbling.on('semantix.dv.highlight-ddzone.off', function(zone) {
        if (zone != dd_group) {
            return;
        }

        (bm_tree.isVisible() ? bm_tree.body : bm_tree_tab).removeClass('semantix-drop-zone');
    });

    g.dropZone = new Ext.dd.DropZone(g.getEl(), {
        ddGroup: dd_group,

        getTargetFromEvent: function(e) {
            return (bm_tree.body && bm_tree.isVisible())? e.getTarget('#' + bm_tree.body.id) : e.getTarget('#' + bm_tree_tab_id, 5);
        },

        onNodeEnter : function(target, dd, e, data) {
            (bm_tree.isVisible() ? bm_tree.body : bm_tree_tab).addClass('semantix-drop-zone-active');
        },

        onNodeOut : function(target, dd, e, data) {
            (bm_tree.isVisible() ? bm_tree.body : bm_tree_tab).removeClass('semantix-drop-zone-active');
        },

        onNodeOver : function(target, dd, e, data) {
            return Ext.dd.DropZone.prototype.dropAllowed;
        },

        onNodeDrop : function(target, dd, e, data) {
            Semantix.Bubbling.fire('semantix.dv.topics.add-bookmark', data.node.id);
            return true;
        }
    });
}
