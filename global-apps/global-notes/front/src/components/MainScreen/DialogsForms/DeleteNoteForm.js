import React from "react";
import {withStyles} from '@material-ui/core';
import formStyles from "./formStyles";
import Button from '@material-ui/core/Button';
import Dialog from '../../common/Dialog/Dialog'
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import connectService from "../../../common/connectService";
import NotesContext from "../../../modules/notes/NotesContext";
import {withSnackbar} from "notistack";
import * as PropTypes from "prop-types";
import DialogContentText from "@material-ui/core/DialogContentText";

class DeleteNoteForm extends React.Component {
  constructor(props, context) {
    super(props, context);
    this.handleDelete = this.handleDelete.bind(this);
  }

  handleDelete = () => {
    return this.props.deleteNote(this.props.noteId)
      .then(this.props.onClose)
      .catch((err) => {
        this.props.enqueueSnackbar(err.message, {
          variant: 'error'
        });
      })
  };

  render() {
    return (
      <Dialog
        open={this.props.open}
        onClose={this.props.onClose}
        aria-labelledby="form-dialog-title"
      >
        <form onSubmit={this.handleSubmit}>
          <DialogTitle
            id="customized-dialog-title"
            onClose={this.props.onClose}
          >
            Delete note
          </DialogTitle>
          <DialogContent>
            <DialogContentText>
              Are you sure you want to delete note?
            </DialogContentText>
          </DialogContent>
          <DialogActions>
            <Button
              className={this.props.classes.actionButton}
              onClick={this.props.onClose}
            >
              No
            </Button>
            <Button
              className={this.props.classes.actionButton}
              color={"primary"}
              variant={"contained"}
              onClick={this.handleDelete}
            >
              Yes
            </Button>
          </DialogActions>
        </form>
      </Dialog>
    );
  }
}

DeleteNoteForm.propTypes = {
  enqueueSnackbar: PropTypes.func.isRequired,
};

export default connectService(
  NotesContext, (state, notesService) => ({
    deleteNote(noteId) {
      return notesService.deleteNote(noteId);
    }
  })
)(
  withSnackbar(withStyles(formStyles)(DeleteNoteForm))
);
