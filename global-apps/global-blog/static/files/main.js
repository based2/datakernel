window.onload = () => {
  function resizeTextArea($element) {
    $element.height($element[0].scrollHeight);
  }

  $('[data-edit]').click((e) => {
    let element = $(e.target.dataset.edit);
    let save = $(`<button class="btn btn-sm btn-primary">save</button>`);
    let cancel = $(`<button class="btn btn-sm btn-outline-primary mx-1">cancel</button>`);
    let textarea = $(`<textarea maxlength="${e.target.dataset.maxlength ? e.target.dataset.maxlength : 120}" required class="form-control" ></textarea>`);

    textarea.keydown((e) => {
      if (e.keyCode === 13 && e.ctrlKey) {
        e.preventDefault();
        save.click();
      }
      if (e.keyCode === 27) {
        cancel.click();
      }
    });
    save.click(() => {
      let object = {};
      object[element[0].dataset.field] = textarea.val();
      fetch(e.target.dataset.editAction, {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        },
        method: 'POST',
        body: JSON.stringify(object)
      }).then(() => location.reload(), console.error);
    });

    element.after(textarea);
    textarea.focus();
    textarea.val(element[0].dataset.value);
    resizeTextArea(textarea);
    element.addClass('d-none');
    textarea.on('input', () => {
      let lines = textarea.val().split('\n').length;
      textarea.attr('rows', lines);
    });

    let dropdown = $(e.target.parentElement);
    dropdown.removeClass('show');

    let row = dropdown.parent().parent();
    row.children().addClass('d-none');
    row.append(cancel);
    row.append(save);

    cancel.click(() => {
      textarea.remove();
      cancel.remove();
      save.remove();
      element.removeClass('d-none');
      row.children().removeClass('d-none');
    });
  });

  $('[data-root-edit]').click((e) => {
    let element = $(e.target.dataset.rootEdit);
    let save = $(`<button class="btn btn-sm btn-primary">save</button>`);
    let cancel = $(`<button class="btn btn-sm btn-outline-primary mx-1">cancel</button>`);
    let textarea = $(`<textarea maxlength="${e.target.dataset.maxlength ? e.target.dataset.maxlength : 120}" required class="form-control" ></textarea>`);
    let attachments = $(`#attachments`);
    let progressBar = $('#progress');

    attachments.show();
    textarea.keydown((e) => {
      if (e.keyCode === 13 && e.ctrlKey) {
        e.preventDefault();
        save.click();
      }
      if (e.keyCode === 27) {
        cancel.click();
      }
    });
    save.click(() => {
      const deleteAttachments = $('[id^="attachment_"]').filter(':checked').map((item, value) => value.dataset.value).get();
      const newAttachments = $('[id$="_attachment"]').get();
      const formData = new FormData();
      window.onbeforeunload = function() {
        return true;
      };
      formData.append("deleteAttachments", deleteAttachments);
      formData.append("content", textarea.val());
      newAttachments.forEach(attachment => {
        if (attachment.files.length) {
          Array.from(attachment.files).forEach(file => {
            formData.append(attachment.name, file)
          })
        }
      });

      progressBar.parent().closest('div').css("display", "");
      textarea.remove();
      cancel.remove();
      save.remove();
      attachments.hide();
      const xhr = new XMLHttpRequest();
      xhr.onload = (exc) => {
        if (xhr.readyState === 4) {
          window.onbeforeunload = null;
          if (xhr.status === 200) {
            location.reload();
          } else {
            console.error(exc);
          }
        }
      };
      xhr.upload.addEventListener('progress', (event) => {
        const progress = Math.round(event.loaded / event.total * 100);
        progressBar.css("width", progress + "%");
        progressBar.html(progress + "%");
      });
      progressBar.show();
      xhr.open("POST", e.target.dataset.editAction, true);
      xhr.send(formData);
    });


    element.after(textarea);
    textarea.focus();
    textarea.val(element[0].dataset.value);
    resizeTextArea(textarea);
    element.addClass('d-none');
    textarea.on('input', () => {
      let lines = textarea.val().split('\n').length;
      textarea.attr('rows', lines);
    });

    let dropdown = $(e.target.parentElement);
    dropdown.removeClass('show');

    let row = dropdown.parent().parent();
    row.children().addClass('d-none');
    row.append(cancel);
    row.append(save);

    cancel.click(() => {
      window.onbeforeunload = null;
      attachments.hide();
      textarea.remove();
      cancel.remove();
      save.remove();
      element.removeClass('d-none');
      row.children().removeClass('d-none');
    });
  });


  // make pressing ctrl+enter submit the current post
  $('[data-post-button]').keydown((e) => {
    if (e.keyCode === 13 && e.ctrlKey) {
      e.preventDefault();
      $(e.target.dataset.postButton).click();
    }
  });

  // store scroll position
  $(window).on('beforeunload', () => localStorage.setItem('scroll', $(document).scrollTop()));
  let scroll = localStorage.getItem('scroll');
  if (scroll) {
    $('html, body').scrollTop(scroll);
  }

  // handle filenames in attachments
  $('.custom-file input').change(function() {
    const thisElement = $(this)[0];
    const files = thisElement.files;
    const type = thisElement.name.split('_')[0];
    const numberOfFiles = files.length;
    const label = $(this).next('.custom-file-label');

    if (numberOfFiles > 1){
      label.html(`[${numberOfFiles} ${type}s]`)
    } else if (numberOfFiles === 1){
      label.html(files[0].name.split('\\').pop());
    } else if (numberOfFiles === 0) {
      label.html(`Attach ${type}s`);
    }
  });

  // handle copy-on-click functionality
  let autocopied = $('.autocopy');
  autocopied.popover({
    content: 'Copied to clipboard',
    placement: 'left',
  });
  autocopied.click(e => {
    let target = $(e.target);
    e.target.select();
    e.target.setSelectionRange(0, 99999); /* For mobile devices */
    document.execCommand("copy");
    setTimeout(() => target.popover('hide'), 1000);
    e.preventDefault();
    return false;
  });

  // handle textarea focus when reply button is pressed
  $('.collapse').on('show.bs.collapse', e => {
    let textarea = $(e.target).find('textarea');
    if (textarea.length) {
      setTimeout(() => textarea.focus(), 0);
    }
  });

  const posts = $("button[id^='post_']");
  posts.each((index, elem) => {
    elem.onclick = function (e) {
      $("div[id='" + elem.id + "']").addClass("target-fade");
      setTimeout(() => $("div[id='" + elem.id + "']").removeClass('target-fade'), 1500);
    }
  });

  //  pagination handling
  const params = location.search
    .slice(1)
    .split('&')
    .filter(value => value)
    .map(p => p.split('='))
    .reduce((obj, [key, value]) => ({...obj, [key]: value}), {});
  const pagination = $("#pagination");
  const currentPage = Number(params.page);
  if (pagination != null && !isNaN(currentPage) && currentPage > 0) {
    const firstPage = $("#firstPage");
    const prevPage = currentPage - 1;
    let maxElements = Number(pagination[0].dataset.maxElements);
    if (maxElements === 0) {
      pagination.toggle("hide");
      return;
    }

    const firstPageNumber = prevPage <= 0 ? currentPage : prevPage;
    firstPage[0].href = window.location.pathname + "?page=" + firstPageNumber + "&size=" + params.size;
    firstPage[0].text = firstPageNumber;
    maxElements -= Math.min(maxElements, (prevPage <= 0 ? params.page : (params.page - 1)) * params.size);
    if (prevPage <= 0) {
      firstPage.parent().addClass("active")
    }

    const secondPageImages = Math.min(maxElements, params.size);
    maxElements -= secondPageImages;
    const secondPage = $("#secondPage");
    const secondPageNumber = prevPage <= 0 ? currentPage + 1 : currentPage;
    secondPage[0].href = window.location.pathname + "?page=" + secondPageNumber + "&size=" + params.size;
    secondPage[0].text = secondPageNumber;
    if (prevPage > 0) {
      secondPage.parent().addClass("active");
    } else {
      if (secondPageImages <= 0) {
        secondPage.parent().addClass("disabled");
      }
    }

    const thirdPageImages = Math.min(maxElements, params.size);
    const third = $("#thirdPage");
    const thirdPageNumber = prevPage <= 0 ? currentPage + 2 : currentPage + 1;
    third[0].href = window.location.pathname + "?page=" + thirdPageNumber + "&size=" + params.size;
    third[0].text = thirdPageNumber;
    if (thirdPageImages === 0) {
      third.parent().addClass("disabled");
    }
  } else {
    pagination.toggle("hide");
  }


  const pageSizeSelect = $("#pageSizeSelect");
  let values = $.map($("#pageSizeSelect option"), function (option) {
    if (Number(pagination[0].dataset.maxElements) < Number($(option).val())) {
      $(option).prop('disabled', true);
    }
    return option.value;
  });
  if (values.includes(params.size)) {
    pageSizeSelect.val(params.size);
  }

  pageSizeSelect.click((e) => {
    const size = pageSizeSelect[0].selectedOptions[0].text;
    window.location.href = window.location.pathname + "?page=" + params.page + "&size=" + size
  });
  // end pagination handling
};

function prevPage() {
  const getParams = location.search
    .slice(1)
    .split('&')
    .filter(value => value)
    .map(p => p.split('='))
    .reduce((obj, [key, value]) => ({...obj, [key]: value}), {});
  window.location.href = window.location.origin + window.location.pathname + "?page=" + (getParams.page > 1 ? getParams.page - 1 : 1) + "&size=" + getParams.size;
}

function nextPage(maxElements) {
  const getParams = location.search
    .slice(1)
    .split('&')
    .filter(value => value)
    .map(p => p.split('='))
    .reduce((obj, [key, value]) => ({...obj, [key]: value}), {});
  const nextPage = Number(getParams.page) + 1;
  const currentSize = getParams.page * getParams.size;
  window.location.href = window.location.origin + window.location.pathname + "?page=" + (currentSize >= maxElements ? getParams.page : nextPage) + "&size=" + getParams.size;
};
