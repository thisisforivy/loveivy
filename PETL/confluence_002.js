AJS.Attachments={showOlderVersions:function(a){a(".attachment-history a").click(function(d){var b=a(this).parents("table.tableview");var c=a(this).parents("tr:first")[0].id.substr(11);var f=a(".history-"+c,b);a(this).toggleClass("icon-section-opened");a(this).toggleClass("icon-section-closed");f.toggleClass("hidden");return AJS.stopEvent(d)})}};AJS.toInit(function(a){var b=a("#more-attachments-link");b.click(function(c){a(".more-attachments").removeClass("hidden");b.addClass("hidden");return AJS.stopEvent(c)});AJS.Attachments.showOlderVersions(a);a(".removeAttachmentLink").click(function(d){var c=a.trim(a(".filename",a(this).parents("tr")).attr("data-filename"));if(confirm(AJS.format(AJS.params.removeAttachmentWarning,c))){return true}else{return AJS.stopEvent(d)}})});
