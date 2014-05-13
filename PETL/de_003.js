AJS.toInit(	function() {
	modifyConfluenceBreadCrums();
});

jQuery.fn.reverse = [].reverse;

function modifyConfluenceBreadCrums() {
	var breadcrumbsContainer = jQuery('#subspace-breadcrumb-path')
	if( jQuery('#subspace-breadcrumb-path').length > 0) {
		if(jQuery('#breadcrumbs').children()[1]) {
			jQuery(jQuery('#breadcrumbs').children()[1]).remove();
		}
		var contextPath = jQuery('#confluence-context-path').attr('content');

		
		breadcrumbsContainer.children('li.breadcrumb').reverse().each(function(){
			jQuery('#breadcrumbs').children(':first').after("<li><a href='"+contextPath+jQuery(this).children('.space-url').text()+"'>"+jQuery(this).children('.space-name').text()+'</a></li>');
		});
	
	
	}
}
