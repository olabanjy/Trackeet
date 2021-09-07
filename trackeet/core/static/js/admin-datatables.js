// Call the dataTables jQuery plugin
// $(document).ready(function() {
//   $('#dataTable').DataTable({
//     "searching": false
//   });
// });

$(document).ready(function() {
  $('#dataTable2').DataTable({
    "order": [[ 3, "desc" ]],
    "searching": false
  });
});